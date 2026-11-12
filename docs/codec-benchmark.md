# Codec benchmark: transit against EDN

Why the store writes transit JSON rather than EDN. Measured on
2026-11-11.

## Setup

- Node 26.12.0, V8 14.6.202.34-node.39, the engine workerd runs; Linux, 13th Gen Intel(R) Core(TM) i7-13700H.
- A clojurific 0.12.15 production bundle built by Vite 8 (`ssr.target`
  `node`, no minification), so the code is what the Worker runs.
- transit-cljs 0.8.280 with transit-js 0.8.874, against `pr-str` and
  `cljs.reader/read-string` of ClojureScript 1.12.145.
- Each figure is the median of seven rounds after one warm-up round:
  20,000 iterations per round for one event, 300 for a page of 100.
- Two events. `small` is the todo event of the example, five keys.
  `rich` is a scheduled story: twelve keys, two nested vectors of maps
  (media, stickers), a set, a caption with an umlaut.

## Results

Microseconds per operation.

| | small, EDN | small, transit | rich, EDN | rich, transit |
| --- | ---: | ---: | ---: | ---: |
| write one | 3.0 | 1.4 | 17.1 | 11.0 |
| read one | 16.3 | 1.7 | 90.3 | 11.6 |
| read one, `JSON.parse` alone | | 0.3 | | 1.9 |
| write a page of 100 | 298.5 | 128.7 | 1728.7 | 987.3 |
| read a page of 100 | 1927.0 | 60.0 | 9116.6 | 438.2 |

Bytes.

| | EDN | transit |
| --- | ---: | ---: |
| small event | 151 | 170 |
| rich event | 810 | 866 |
| page of 100 small | 15,201 | 11,557 |
| page of 100 rich | 81,101 | 57,793 |

Both codecs round-trip both events exactly.

## Reading

- Reading is where it counts. Transit reads eight to ten times faster
  for one event and twenty times faster for a page, because the parse is
  the native `JSON.parse` and only the decoding of tags and keys runs in
  ClojureScript. Transit's decoding costs about five times the native
  parse; the EDN reader costs about forty times.
- Writing is 1.6 to 2 times faster.
- The Object runs one request at a time, so this is time the stream is
  blocked for. A rebuild of 100,000 rich events reads them in about
  1.2 s with transit and 9 s with EDN, against 30 s of CPU per request.
  A page read over RPC blocks the stream for under half a millisecond
  instead of 9 ms.
- One event is 7 to 12 percent larger as transit, from the `~:` prefix
  of every keyword. A page is 25 to 30 percent smaller, because transit
  caches keys repeated within one write.
- Rows stay readable in `sqlite3`:
  `["^ ","~:event/type","~:todo/added","~:todo/id","6d06…","~:todo/text","Ship it","~:event/number",1,"~:event/at","2026-11-11T09:34:02.031Z"]`

## Reproduce

In `example/`, put the namespace below under `src/example/bench.cljs`,
build it with the Vite config that follows, and run it. A checkout that
has built before caches its classpath in `.cljf/cpcache`; `npx cljf
--force classpath` resolves a new dependency.

```sh
npx vite build --config vite.bench.config.mjs
node -e "import('./dist/bench/bench.js').then(m => m.run())"
```

```js
// vite.bench.config.mjs
import cljf from 'clojurific/vite';
export default {
  plugins: [cljf({ outputDir: 'target/bench', manifest: false,
                   compilerOptions: { 'js-entries': { bench: { exports: { run: "'example.bench/run" } } } } })],
  build: { ssr: true, outDir: 'dist/bench', minify: false,
           rolldownOptions: { input: {}, external: [/^node:/], output: { entryFileNames: '[name].js', codeSplitting: false } } },
  ssr: { target: 'node', noExternal: true },
};
```

```clojure
(ns example.bench
  (:require [cljs.reader :as reader]
            [cognitect.transit :as t]))

(def small
  {:event/type :todo/added
   :todo/id "6d06dd65-4da6-4d91-9f8a-ae7bb1c02494"
   :todo/text "Ship it"
   :event/number 1
   :event/at "2026-11-11T09:34:02.031Z"})

(def rich
  {:event/type :story/scheduled
   :story/id "c4f0c8d2-2b4a-4f2e-9a1f-1c2d3e4f5a6b"
   :story/account "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
   :story/at "2026-11-12T08:30:00.000Z"
   :story/media [{:media/id "m1" :media/kind :image :media/url "https://blob.example/abc" :media/width 1080 :media/height 1920}
                 {:media/id "m2" :media/kind :video :media/url "https://blob.example/def" :media/duration 14.5}]
   :story/caption "Behind the scenes of today's shoot, with the whole team on location in Köln."
   :story/stickers [{:sticker/kind :poll :sticker/question "Which one?" :sticker/options ["A" "B"] :sticker/x 0.5 :sticker/y 0.7}
                    {:sticker/kind :link :sticker/url "https://storrito.com" :sticker/x 0.2 :sticker/y 0.9}]
   :story/crosspost #{:facebook}
   :event/number 4711
   :event/at "2026-11-11T09:34:02.031Z"})

(def w (t/writer :json))
(def r (t/reader :json))
(def sink (volatile! 0))

(defn ms [f n]
  (let [t0 (js/performance.now)]
    (dotimes [_ n] (vswap! sink + (count (f))))
    (- (js/performance.now) t0)))

(defn median [xs] (nth (sort xs) (quot (count xs) 2)))

(defn bench [label f n]
  (ms f n)
  (let [m (median (vec (repeatedly 7 #(ms f n))))]
    (println (str "  " label ": " (.toFixed (/ (* 1000 m) n) 1) " µs/op"))))

(defn run []
  (doseq [[name event] [["small" small] ["rich" rich]]]
    (let [edn (pr-str event)
          tj (t/write w event)
          page (vec (repeat 100 event))
          edn-page (pr-str page)
          tj-page (t/write w page)]
      (println (str name " event: round trip edn " (= event (reader/read-string edn)) ", transit " (= event (t/read r tj))))
      (println (str name " event bytes: edn " (count edn) ", transit " (count tj) " | page of 100: edn " (count edn-page) ", transit " (count tj-page)))
      (bench "write one, edn" #(pr-str event) 20000)
      (bench "write one, transit" #(t/write w event) 20000)
      (bench "read one, edn" #(reader/read-string edn) 20000)
      (bench "read one, transit" #(t/read r tj) 20000)
      (bench "read one, JSON.parse alone" #(js/JSON.parse tj) 20000)
      (bench "write page of 100, edn" #(pr-str page) 300)
      (bench "write page of 100, transit" #(t/write w page) 300)
      (bench "read page of 100, edn" #(reader/read-string edn-page) 300)
      (bench "read page of 100, transit" #(t/read r tj-page) 300)
      (println))))
```

# Example: a todo stream

The library used as a Worker: `src/example/todo.cljs` is the domain,
`src/example/worker.cljs` defines the `Stream` Object with it, and
`test/worker/` exercises the Object inside workerd.

```sh
npm ci
npm test        # builds the bundle, then runs the Object inside workerd
npm run deploy  # wrangler deploy
```

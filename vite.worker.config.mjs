import cljf from 'clojurific/vite';

// The Worker and its Durable Object class, one bundle: `vite build
// --config vite.worker.config.mjs` writes dist/worker/worker-event-sourcing.js
// with the Worker's default export and the Stream class, which
// wrangler.jsonc names as main and as the Durable Object's class.
export default {
  plugins: [
    cljf({
      outputDir: 'target/worker',
      compilerOptions: {
        'js-entries': {
          'worker-event-sourcing': {
            exports: {
              default: "'event-sourcing.worker/handler",
              Stream: "'event-sourcing.worker/Stream",
            },
          },
        },
      },
      manifest: false,
    }),
  ],
  build: {
    ssr: true,
    outDir: 'dist/worker',
    rolldownOptions: {
      input: {},
      external: [/^node:/],
      output: { entryFileNames: '[name].js', codeSplitting: false },
    },
  },
  ssr: {
    target: 'webworker',
    noExternal: true,
  },
};

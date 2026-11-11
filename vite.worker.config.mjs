import cljf from 'clojurific/vite';

// The two Durable Object classes, one bundle: `vite build --config
// vite.worker.config.mjs` writes dist/worker/worker-event-sourcing.js
// exporting Log and Projection, which wrangler.jsonc names as the classes.
export default {
  plugins: [
    cljf({
      outputDir: 'target/worker',
      compilerOptions: {
        'js-entries': {
          'worker-event-sourcing': {
            exports: {
              Log: "'event-sourcing.objects/Log",
              Projection: "'event-sourcing.objects/Projection",
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
      external: [/^node:/, /^cloudflare:/],
      output: { entryFileNames: '[name].js', codeSplitting: false },
    },
  },
  ssr: {
    target: 'webworker',
    noExternal: true,
  },
};

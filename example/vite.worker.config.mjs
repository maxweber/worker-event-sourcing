import cljf from 'clojurific/vite';

// The Durable Object class, one bundle: `vite build --config
// vite.worker.config.mjs` writes dist/worker/example.js exporting
// Stream, which wrangler.jsonc names as the class.
export default {
  plugins: [
    cljf({
      outputDir: 'target/worker',
      compilerOptions: {
        'js-entries': {
          example: {
            exports: {
              Stream: "'example.worker/Stream",
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

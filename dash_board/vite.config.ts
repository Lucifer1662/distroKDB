import solid from "solid-start/vite";
// import { defineConfig } from "vite";

// export default defineConfig({
//   plugins: [solid({ ssr: false })],
// });


import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

export default defineConfig({
  plugins: [solidPlugin({ssr: false})],
  // plugins: [solid({ ssr: false })],

  server: {
    port: 3000,
  },
  build: {
    target: 'esnext',
  },
});

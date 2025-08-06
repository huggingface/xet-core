import adapter from "@sveltejs/adapter-static";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

/** @type {import('@sveltejs/kit').Config} */
const config = {
  // Consult https://svelte.dev/docs/kit/integrations
  // for more information about preprocessors
  preprocess: vitePreprocess(),

  kit: {
    // Static adapter for generating static files
    adapter: adapter({
      // Output directory
      pages: "dist",
      assets: "dist",
      fallback: null,
      precompress: false,
      strict: true,
    }),
  },
};

export default config;

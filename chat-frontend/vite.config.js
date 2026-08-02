import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  plugins: [react()],
  // `@/foo` resolves to `<repo>/src/foo`. Used everywhere a relative import
  // would otherwise climb 3+ levels — keeps cross-package imports
  // legible. Same-folder and one-up imports stay relative.
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
    dedupe: ['react', 'react-dom'],
  },
  build: {
    rollupOptions: {
      output: {
        // Force React/ReactDOM/scheduler into ONE shared chunk so the entry AND every
        // async (React.lazy) chunk reference the SAME React module instance. Without
        // this, Rollup's code-split produced two React instances: react-dom bound its
        // dispatcher to one, while the lazy dialog chunks imported the other (null
        // dispatcher) → "Invalid hook call" (#321) the moment any lazy dialog mounts
        // (New Section, Create Room, Manage Members, …). Prod-build-only — vitest uses
        // a single module graph, so unit tests never surfaced it. resolve.dedupe alone
        // did not fix it (bare `react` already resolved to one copy); the dup was a
        // chunk-boundary artifact, which a manual vendor chunk collapses.
        manualChunks(id) {
          if (/node_modules\/(react|react-dom|scheduler)\//.test(id)) return 'react-vendor'
        },
      },
    },
  },
  server: {
    port: 3000,
  },
})

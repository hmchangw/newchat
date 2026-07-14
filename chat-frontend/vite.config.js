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
  },
  server: {
    port: 3000,
  },
})

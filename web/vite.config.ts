import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('@mui') || id.includes('@emotion')) {
            return 'vendor-mui';
          }
          if (id.includes('chart.js') || id.includes('react-chartjs-2') || id.includes('chartjs-adapter-date-fns')) {
            return 'vendor-charts';
          }
          if (id.includes('node_modules/react/') || id.includes('node_modules/react-dom/') || id.includes('node_modules/react-router/') || id.includes('node_modules/react-router-dom/')) {
            return 'vendor-react';
          }
          if (id.includes('@bufbuild') || id.includes('@connectrpc')) {
            return 'vendor-connect';
          }
        }
      }
    }
  },
  server: {
    host: true,
    watch: {
      usePolling: true,
    },
    proxy: {
      '/swarun.v1.ControllerService': {
        target: process.env.VITE_API_URL || 'http://localhost:8080',
        changeOrigin: true,
      }
    }
  }
})

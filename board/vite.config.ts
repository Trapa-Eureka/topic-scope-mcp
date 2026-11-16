import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the page comes from Vite (port 5173) and the WebSocket from the server in board mode (port 8090).
// Start the server with TOPIC_SCOPE_BOARD_DEV_ORIGIN=http://localhost:5173 so it accepts this page.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ws": { target: "ws://127.0.0.1:8090", ws: true, changeOrigin: false },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});

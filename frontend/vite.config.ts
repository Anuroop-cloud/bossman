import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: same-origin proxy to the FastAPI backend (no credentials in the bundle,
// no CORS needed). Backend address is a dev-server setting only.
const backend = process.env.BOSSMAN_BACKEND ?? "127.0.0.1:5010";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3010,
    proxy: {
      "/api": { target: `http://${backend}`, changeOrigin: false },
      "/ws": { target: `ws://${backend}`, ws: true, changeOrigin: false },
    },
  },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});

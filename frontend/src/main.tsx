import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import { LiveProvider } from "./lib/live";
import { SessionProvider } from "./lib/session";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <BrowserRouter>
      <SessionProvider>
        <LiveProvider>
          <App />
        </LiveProvider>
      </SessionProvider>
    </BrowserRouter>
  </React.StrictMode>
);

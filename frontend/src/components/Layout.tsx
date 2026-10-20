import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useLive } from "../lib/live";
import { useQuery } from "../lib/useQuery";
import { TokenLogin } from "./TokenLogin";
import { NewTaskDialog } from "./NewTaskDialog";
import {
  LayoutDashboard, Users, CheckSquare, AlertTriangle, Box, Settings, ShieldCheck, Scale, Cpu, Rocket, Plus, Sun, Moon,
} from "lucide-react";
import clsx from "clsx";

type Theme = "dark" | "light";

function initialTheme(): Theme {
  try {
    const saved = localStorage.getItem("bossman-theme");
    if (saved === "dark" || saved === "light") return saved;
  } catch { /* storage unavailable */ }
  return window.matchMedia?.("(prefers-color-scheme: light)").matches ? "light" : "dark";
}

const TITLES: Record<string, string> = {
  "": "Command Center", agents: "Agents", tasks: "Tasks", traces: "Trace", incidents: "Incidents", approvals: "Approvals",
  resources: "Resources", operations: "Operations", governance: "Governance", system: "System", start: "Get started",
};

export function Layout() {
  const live = useLive();
  const loc = useLocation();
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [newTask, setNewTask] = useState(false);
  const pending = useQuery<{ approvals: unknown[] }>("/api/approvals?status=pending");
  const overview = useQuery<{ status: string; agents: { total: number } }>("/api/overview");
  const pendingCount = pending.data?.approvals.length ?? 0;

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try { localStorage.setItem("bossman-theme", theme); } catch { /* ignore */ }
  }, [theme]);

  const link = ({ isActive }: { isActive: boolean }) => clsx("nav-link", isActive && "active");
  const section = loc.pathname.split("/")[1] ?? "";
  const status = overview.data?.status ?? "…";
  const statusTone = status === "HEALTHY" ? "ok" : status === "CRITICAL" ? "bad" : "warn";

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" />
          <div>BOSSman<small>agent control plane</small></div>
        </div>
        <button className="new-task" onClick={() => setNewTask(true)}><Plus size={16} /> New task</button>

        <nav style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <NavLink to="/" className={link} end><LayoutDashboard size={16} /> Command Center</NavLink>
          <NavLink to="/start" className={link}><Rocket size={16} /> Get started</NavLink>

          <div className="nav-section">Fleet</div>
          <NavLink to="/agents" className={link}><Users size={16} /> Agents</NavLink>
          <NavLink to="/operations" className={link}><Cpu size={16} /> Operations</NavLink>
          <NavLink to="/resources" className={link}><Box size={16} /> Resources</NavLink>

          <div className="nav-section">Work</div>
          <NavLink to="/tasks" className={link}><CheckSquare size={16} /> Tasks</NavLink>
          <NavLink to="/incidents" className={link}><AlertTriangle size={16} /> Incidents</NavLink>

          <div className="nav-section">Control</div>
          <NavLink to="/approvals" className={link}>
            <ShieldCheck size={16} /> Approvals
            {pendingCount > 0 && <span className="badge warn" style={{ marginLeft: "auto" }}>{pendingCount}</span>}
          </NavLink>
          <NavLink to="/governance" className={link}><Scale size={16} /> Governance</NavLink>
          <NavLink to="/system" className={link}><Settings size={16} /> System</NavLink>
        </nav>

        <div className="sidebar-foot" style={{ display: "grid", gap: 12 }}>
          <TokenLogin />
          <div className="dim">BOSSman v0.2.0</div>
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <span className="crumbs">BOSSman / <strong style={{ color: "var(--text)" }}>{TITLES[section] ?? section}</strong></span>
          <span className="spacer" />
          <span className="status-pill"><span className={clsx("dot", statusTone, "pulse")} /> {status}
            · {overview.data?.agents.total ?? 0} agents</span>
          <span className="status-pill">
            <span className={clsx("dot", live.status === "open" ? "ok" : live.status === "stale" ? "bad" : "warn")} />
            {live.status === "open" ? "Live" : live.status === "stale" ? "Disconnected" : "Connecting…"}
          </span>
          <button className="icon-btn" aria-label="Toggle theme" onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>
            {theme === "dark" ? <Sun size={15} /> : <Moon size={15} />}
          </button>
        </header>
        <div className="content"><Outlet /></div>
      </main>
      {newTask && <NewTaskDialog onClose={() => setNewTask(false)} />}
    </div>
  );
}

import { NavLink, Outlet } from "react-router-dom";
import { useLive } from "../lib/live";
import { LayoutDashboard, Users, CheckSquare, AlertTriangle, Box, Settings, Wifi, WifiOff } from "lucide-react";
import clsx from "clsx";

export function Layout() {
  const live = useLive();
  
  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" />
          <span>BOSSman</span>
        </div>
        
        <nav style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <NavLink to="/" className={({ isActive }) => clsx("nav-link", isActive && "active")} end>
            <LayoutDashboard size={16} /> Command Center
          </NavLink>
          <NavLink to="/agents" className={({ isActive }) => clsx("nav-link", isActive && "active")}>
            <Users size={16} /> Agents
          </NavLink>
          <NavLink to="/tasks" className={({ isActive }) => clsx("nav-link", isActive && "active")}>
            <CheckSquare size={16} /> Tasks
          </NavLink>
          <NavLink to="/incidents" className={({ isActive }) => clsx("nav-link", isActive && "active")}>
            <AlertTriangle size={16} /> Incidents
          </NavLink>
          <NavLink to="/resources" className={({ isActive }) => clsx("nav-link", isActive && "active")}>
            <Box size={16} /> Resources
          </NavLink>
          
          <div className="nav-sep" />
          
          <NavLink to="/system" className={({ isActive }) => clsx("nav-link", isActive && "active")}>
            <Settings size={16} /> System
          </NavLink>
        </nav>
        
        <div className="sidebar-foot">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            {live.status === "open" ? (
              <>
                <Wifi size={14} className="tone-ok" />
                <span className="tone-ok">Connected</span>
              </>
            ) : live.status === "connecting" || live.status === "reconnecting" ? (
              <>
                <Wifi size={14} className="tone-warn" />
                <span className="tone-warn">Connecting...</span>
              </>
            ) : (
              <>
                <WifiOff size={14} className="tone-bad" />
                <span className="tone-bad">Disconnected</span>
              </>
            )}
          </div>
          <div className="dim">v0.1.0-alpha</div>
        </div>
      </aside>
      
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}

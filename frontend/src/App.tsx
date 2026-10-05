import { Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import { CommandCenter } from "./pages/CommandCenter";
import { Agents } from "./pages/Agents";
import { AgentDetail } from "./pages/AgentDetail";
import { Tasks } from "./pages/Tasks";
import { Incidents } from "./pages/Incidents";
import { IncidentDetail } from "./pages/IncidentDetail";
import { Resources } from "./pages/Resources";
import { System } from "./pages/System";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<CommandCenter />} />
        <Route path="agents" element={<Agents />} />
        <Route path="agents/:id" element={<AgentDetail />} />
        <Route path="tasks" element={<Tasks />} />
        <Route path="incidents" element={<Incidents />} />
        <Route path="incidents/:id" element={<IncidentDetail />} />
        <Route path="resources" element={<Resources />} />
        <Route path="system" element={<System />} />
      </Route>
    </Routes>
  );
}

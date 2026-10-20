import { Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import { CommandCenter } from "./pages/CommandCenter";
import { Agents } from "./pages/Agents";
import { AgentDetail } from "./pages/AgentDetail";
import { Tasks } from "./pages/Tasks";
import { TaskDetail } from "./pages/TaskDetail";
import { Incidents } from "./pages/Incidents";
import { IncidentDetail } from "./pages/IncidentDetail";
import { Resources } from "./pages/Resources";
import { System } from "./pages/System";
import { Approvals } from "./pages/Approvals";
import { Governance } from "./pages/Governance";
import { Operations } from "./pages/Operations";
import { Trace } from "./pages/Trace";
import { GetStarted } from "./pages/GetStarted";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<CommandCenter />} />
        <Route path="agents" element={<Agents />} />
        <Route path="agents/:id" element={<AgentDetail />} />
        <Route path="tasks" element={<Tasks />} />
        <Route path="tasks/:id" element={<TaskDetail />} />
        <Route path="traces/:id" element={<Trace />} />
        <Route path="incidents" element={<Incidents />} />
        <Route path="incidents/:id" element={<IncidentDetail />} />
        <Route path="approvals" element={<Approvals />} />
        <Route path="resources" element={<Resources />} />
        <Route path="operations" element={<Operations />} />
        <Route path="governance" element={<Governance />} />
        <Route path="system" element={<System />} />
        <Route path="start" element={<GetStarted />} />
      </Route>
    </Routes>
  );
}

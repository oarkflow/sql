import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import Index from "./pages/Index";
import Integrations from "./pages/Integrations";
import ETLPipelines from "./pages/ETLPipelines";
import QueryEditor from "./pages/QueryEditor";
import Runs from "./pages/Runs";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout><Index /></Layout>} />
          <Route path="/integrations" element={<Layout><Integrations /></Layout>} />
          <Route path="/pipelines" element={<Layout><ETLPipelines /></Layout>} />
          <Route path="/pipelines/new" element={<ETLPipelines />} />
          <Route path="/pipelines/:id/edit" element={<ETLPipelines />} />
          <Route path="/query-editor" element={<Layout><QueryEditor /></Layout>} />
          <Route path="/runs" element={<Layout><Runs /></Layout>} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;

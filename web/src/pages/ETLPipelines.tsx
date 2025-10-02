import { useParams, useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { ETLWizard } from "@/components/etl/ETLWizard";
import ETLPipelinesList from "./ETLPipelinesList";
import { Layout } from "@/components/Layout";

export default function ETLPipelines() {
    const { id } = useParams();
    const location = useLocation();
    const isNewPipeline = location.pathname === "/pipelines/new";
    const isEditPipeline = !!id;

    const { data: pipeline } = useQuery({
        queryKey: ["pipeline", id],
        queryFn: () => api.getPipeline(id!),
        enabled: isEditPipeline,
    });

    if (isNewPipeline || isEditPipeline) {
        return (
            <Layout>
                <ETLWizard pipeline={isEditPipeline ? pipeline : undefined} />
            </Layout>
        );
    }

    return <ETLPipelinesList />;
}

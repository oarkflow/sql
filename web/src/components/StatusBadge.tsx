import { Badge } from "@/components/ui/badge";
import { IntegrationStatus, RunStatus } from "@/types/etl";
import { CheckCircle2, XCircle, AlertCircle, Loader2, Clock } from "lucide-react";

interface StatusBadgeProps {
  status: IntegrationStatus | RunStatus;
  size?: "sm" | "md";
}

export function StatusBadge({ status, size = "md" }: StatusBadgeProps) {
  const iconSize = size === "sm" ? "h-3 w-3" : "h-4 w-4";

  const config = {
    connected: {
      variant: "default" as const,
      icon: <CheckCircle2 className={iconSize} />,
      label: "Connected",
      className: "bg-success text-success-foreground",
    },
    disconnected: {
      variant: "secondary" as const,
      icon: <AlertCircle className={iconSize} />,
      label: "Disconnected",
      className: "bg-muted text-muted-foreground",
    },
    error: {
      variant: "destructive" as const,
      icon: <XCircle className={iconSize} />,
      label: "Error",
      className: "bg-destructive text-destructive-foreground",
    },
    success: {
      variant: "default" as const,
      icon: <CheckCircle2 className={iconSize} />,
      label: "Success",
      className: "bg-success text-success-foreground",
    },
    failed: {
      variant: "destructive" as const,
      icon: <XCircle className={iconSize} />,
      label: "Failed",
      className: "bg-destructive text-destructive-foreground",
    },
    running: {
      variant: "secondary" as const,
      icon: <Loader2 className={`${iconSize} animate-spin`} />,
      label: "Running",
      className: "bg-primary text-primary-foreground",
    },
    pending: {
      variant: "secondary" as const,
      icon: <Clock className={iconSize} />,
      label: "Pending",
      className: "bg-warning text-warning-foreground",
    },
  };

  const { icon, label, className } = config[status] || config.disconnected;

  return (
    <Badge className={`gap-1.5 ${className}`}>
      {icon}
      <span>{label}</span>
    </Badge>
  );
}

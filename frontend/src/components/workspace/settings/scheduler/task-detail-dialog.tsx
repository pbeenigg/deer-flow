"use client";

import { useState } from "react";
import { Clock, ChevronDown, ChevronUp, CheckCircle2, XCircle, Loader2, Mail, MessageCircle, Send, Bell } from "lucide-react";

import { useI18n } from "@/core/i18n/hooks";
import { useTaskExecutions } from "@/core/scheduler/hooks";
import type { ScheduledTask } from "@/core/scheduler/api";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

interface TaskDetailDialogProps {
  task: ScheduledTask | null;
  onOpenChange: (open: boolean) => void;
}

const CHANNEL_ICONS: Record<string, React.ElementType> = {
  email: Mail,
  wechat: MessageCircle,
  telegram: Send,
  dingtalk: Bell,
};

export function TaskDetailDialog({ task, onOpenChange }: TaskDetailDialogProps) {
  const { t } = useI18n();
  const { executions } = useTaskExecutions(task?.task_id || null);
  const [showExecutions, setShowExecutions] = useState(false);

  if (!task) return null;

  function formatTimestamp(ts: number | null): string {
    if (!ts) return t.settings.scheduler.never;
    return new Date(ts * 1000).toLocaleString();
  }

  function formatDuration(start: number, end: number | null): string {
    if (!end) return "...";
    const seconds = Math.round(end - start);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}m ${remainingSeconds}s`;
  }

  const statusIcons: Record<string, React.ElementType> = {
    running: Loader2,
    success: CheckCircle2,
    failed: XCircle,
  };

  const statusColors: Record<string, string> = {
    running: "text-blue-500",
    success: "text-green-500",
    failed: "text-red-500",
  };

  const channelLabels: Record<string, string> = {
    email: t.settings.scheduler.channelEmail,
    wechat: t.settings.scheduler.channelWechat,
    telegram: t.settings.scheduler.channelTelegram,
    dingtalk: t.settings.scheduler.channelDingtalk,
  };

  return (
    <Dialog open={!!task} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Clock className="size-5 text-primary" />
            {task.task_name}
          </DialogTitle>
        </DialogHeader>
        <ScrollArea className="max-h-[60vh] pr-2">
          <div className="space-y-5">
            {/* 信息网格 */}
            <div className="grid grid-cols-2 gap-3">
              <InfoItem label={t.settings.scheduler.scheduleType} value={task.schedule_type} />
              <InfoItem label={t.settings.scheduler.timezone} value={task.timezone} />
              <InfoItem label={t.settings.scheduler.nextRunAt} value={formatTimestamp(task.next_run_at)} />
              <InfoItem label={t.settings.scheduler.lastRunAt} value={formatTimestamp(task.last_run_at)} />
            </div>

            {/* 推送渠道 */}
            <div className="space-y-1.5">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.notifyChannels}
              </div>
              <div className="flex flex-wrap gap-1.5">
                {task.notify_channels.map((ch) => {
                  const Icon = CHANNEL_ICONS[ch] || Bell;
                  return (
                    <span key={ch} className="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs">
                      <Icon className="size-3" />
                      {channelLabels[ch] || ch}
                    </span>
                  );
                })}
              </div>
            </div>

            {/* 提示词 */}
            <div className="space-y-1.5">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.taskPrompt}
              </div>
              <div className="rounded-lg border bg-muted/30 p-3 text-sm text-foreground/80 whitespace-pre-wrap leading-relaxed">
                {task.task_prompt}
              </div>
            </div>

            {/* 执行历史 */}
            <div className="space-y-2">
              <button
                type="button"
                className="flex w-full items-center justify-between rounded-lg border px-3 py-2.5 text-sm font-medium transition-colors hover:bg-accent/50"
                onClick={() => setShowExecutions(!showExecutions)}
              >
                <span className="flex items-center gap-2">
                  <Clock className="size-4 text-muted-foreground" />
                  {t.settings.scheduler.executions}
                  <span className="rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">{executions.length}</span>
                </span>
                {showExecutions ? <ChevronUp className="size-4" /> : <ChevronDown className="size-4" />}
              </button>

              {showExecutions && (
                executions.length === 0 ? (
                  <div className="py-4 text-center text-sm text-muted-foreground">{t.settings.scheduler.noExecutions}</div>
                ) : (
                  <div className="space-y-1.5">
                    {executions.map((exec) => {
                      const StatusIcon = statusIcons[exec.status] || Clock;
                      return (
                        <div key={exec.id} className="rounded-lg border p-3 space-y-2">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              <StatusIcon className={cn("size-4", statusColors[exec.status], exec.status === "running" && "animate-spin")} />
                              <span className="text-xs font-medium capitalize">{exec.status}</span>
                            </div>
                            <div className="flex items-center gap-2 text-xs text-muted-foreground">
                              <span>{formatTimestamp(exec.started_at)}</span>
                              <span>{formatDuration(exec.started_at, exec.finished_at)}</span>
                            </div>
                          </div>
                          {exec.error_message && (
                            <div className="rounded-md bg-red-50 dark:bg-red-900/10 p-2 text-xs text-red-600 dark:text-red-400">
                              {exec.error_message}
                            </div>
                          )}
                          {exec.result_content && (
                            <div className="rounded-md bg-muted/50 p-2 text-xs text-foreground/70 line-clamp-4 whitespace-pre-wrap">
                              {exec.result_content}
                            </div>
                          )}
                          {exec.notify_status && (
                            <div className="flex flex-wrap gap-1">
                              {Object.entries(exec.notify_status).map(([channel, result]) => (
                                <span
                                  key={channel}
                                  className={cn(
                                    "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium",
                                    result.status === "success"
                                      ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                                      : "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
                                  )}
                                >
                                  {result.status === "success" ? <CheckCircle2 className="size-2.5" /> : <XCircle className="size-2.5" />}
                                  {channel}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )
              )}
            </div>
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}

function InfoItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-muted/20 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="mt-0.5 text-sm font-medium truncate">{value}</div>
    </div>
  );
}

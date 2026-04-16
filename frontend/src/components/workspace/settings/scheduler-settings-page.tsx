"use client";

import { useState } from "react";
import { Clock, Plus, Trash2, Play, Pause, Eye, Pencil, Bell, Mail, MessageCircle, Send, Zap } from "lucide-react";
import { toast } from "sonner";

import { useI18n } from "@/core/i18n/hooks";
import { useScheduledTasks, useSchedulerStatus, useUpdateTaskStatus, useDeleteScheduledTask, useTaskTemplates, useTriggerTask } from "@/core/scheduler/hooks";
import type { ScheduledTask } from "@/core/scheduler/api";
import { SettingsSection } from "./settings-section";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { TaskFormDialog } from "./scheduler/task-form-dialog";
import { TaskDetailDialog } from "./scheduler/task-detail-dialog";
import { cn } from "@/lib/utils";

const CHANNEL_ICONS: Record<string, React.ElementType> = {
  email: Mail,
  wechat: MessageCircle,
  telegram: Send,
  dingtalk: Bell,
};

export function SchedulerSettingsPage() {
  const { t } = useI18n();
  const { tasks, isLoading } = useScheduledTasks();
  const { status } = useSchedulerStatus();
  const updateStatus = useUpdateTaskStatus();
  const deleteTask = useDeleteScheduledTask();
  const triggerTaskMutation = useTriggerTask();
  const { templates } = useTaskTemplates();

  const [formDialogOpen, setFormDialogOpen] = useState(false);
  const [editTask, setEditTask] = useState<ScheduledTask | null>(null);
  const [selectedTemplate, setSelectedTemplate] = useState<string>("");
  const [detailTask, setDetailTask] = useState<ScheduledTask | null>(null);
  const [deleteConfirmTask, setDeleteConfirmTask] = useState<ScheduledTask | null>(null);

  function handleToggleTask(task: ScheduledTask) {
    const newStatus = task.status === "active" ? "paused" : "active";
    updateStatus.mutate(
      { taskId: task.task_id, status: newStatus },
      {
        onSuccess: () => toast.success(newStatus === "active" ? t.settings.scheduler.resumeTask : t.settings.scheduler.pauseTask),
      },
    );
  }

  function handleDeleteTask() {
    if (!deleteConfirmTask) return;
    deleteTask.mutate(deleteConfirmTask.task_id, {
      onSuccess: () => {
        toast.success(t.settings.scheduler.deleteSuccess);
        setDeleteConfirmTask(null);
      },
    });
  }

  function handleTemplateSelect(templateId: string) {
    setSelectedTemplate(templateId);
    setEditTask(null);
    setFormDialogOpen(true);
  }

  function handleCreateNew() {
    setSelectedTemplate("");
    setEditTask(null);
    setFormDialogOpen(true);
  }

  function handleEditTask(task: ScheduledTask) {
    setEditTask(task);
    setSelectedTemplate("");
    setFormDialogOpen(true);
  }

  function handleTriggerTask(task: ScheduledTask) {
    if (triggerTaskMutation.isPending) return;
    triggerTaskMutation.mutate(task.task_id, {
      onSuccess: () => toast.success(t.settings.scheduler.triggerSuccess),
      onError: () => toast.error(t.settings.scheduler.triggerFailed),
    });
  }

  function handleFormDialogClose(open: boolean) {
    setFormDialogOpen(open);
    if (!open) {
      setEditTask(null);
      setSelectedTemplate("");
    }
  }

  function formatTimestamp(ts: number | null): string {
    if (!ts) return t.settings.scheduler.never;
    return new Date(ts * 1000).toLocaleString();
  }

  function formatScheduleSummary(task: ScheduledTask): string {
    const cfg = task.schedule_config;
    switch (task.schedule_type) {
      case "daily":
        return `${String(cfg.hour ?? 0).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")} ${t.settings.scheduler.scheduleDaily}`;
      case "weekly":
        return `${t.settings.scheduler.daysOfWeek[(cfg.day_of_week as string) as keyof typeof t.settings.scheduler.daysOfWeek] || cfg.day_of_week} ${String(cfg.hour ?? 0).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")}`;
      case "once":
        return `${String(cfg.hour ?? 0).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")} ${t.settings.scheduler.scheduleOnce}`;
      case "interval":
        return `${cfg.minutes ?? 60}${t.settings.scheduler.minute} ${t.settings.scheduler.scheduleInterval}`;
      case "cron":
        return `${cfg.expression}`;
      default:
        return task.schedule_type;
    }
  }

  const channelLabels: Record<string, string> = {
    email: t.settings.scheduler.channelEmail,
    wechat: t.settings.scheduler.channelWechat,
    telegram: t.settings.scheduler.channelTelegram,
    dingtalk: t.settings.scheduler.channelDingtalk,
  };

  if (isLoading) {
    return (
      <SettingsSection title={t.settings.scheduler.title} description={t.settings.scheduler.description}>
        <div className="flex items-center justify-center py-8 text-muted-foreground">{t.common.loading}</div>
      </SettingsSection>
    );
  }

  const activeTasks = tasks.filter((task) => task.status !== "deleted");

  return (
    <SettingsSection title={t.settings.scheduler.title} description={t.settings.scheduler.description}>
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className={cn(
              "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium",
              status?.enabled
                ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                : "bg-muted text-muted-foreground",
            )}>
              <span className={cn(
                "size-1.5 rounded-full",
                status?.running ? "bg-green-500 animate-pulse" : "bg-gray-400",
              )} />
              {status?.enabled ? t.settings.scheduler.statusRunning : t.settings.scheduler.statusDisabled}
            </span>
            {status?.enabled && status.active_tasks > 0 && (
              <span className="text-xs text-muted-foreground">
                {status.active_tasks} {t.settings.scheduler.activeTasks}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {templates.length > 0 && (
              <Select value={selectedTemplate} onValueChange={handleTemplateSelect}>
                <SelectTrigger className="w-[150px] h-8 text-xs">
                  <SelectValue placeholder={t.settings.scheduler.fromTemplate} />
                </SelectTrigger>
                <SelectContent>
                  {templates.map((tpl) => (
                    <SelectItem key={tpl.id} value={tpl.id}>{tpl.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            <Button size="sm" onClick={handleCreateNew}>
              <Plus className="size-4 mr-1.5" />
              {t.settings.scheduler.createTask}
            </Button>
          </div>
        </div>

        {activeTasks.length === 0 ? (
          <Empty>
            <EmptyHeader>
              <EmptyMedia variant="icon"><Clock className="size-5" /></EmptyMedia>
              <EmptyTitle>{t.settings.scheduler.emptyTitle}</EmptyTitle>
              <EmptyDescription>{t.settings.scheduler.emptyDescription}</EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <Button size="sm" onClick={handleCreateNew}>
                <Plus className="size-4 mr-1.5" />
                {t.settings.scheduler.createTask}
              </Button>
            </EmptyContent>
          </Empty>
        ) : (
          <div className="space-y-2">
            {activeTasks.map((task) => (
              <TaskCard
                key={task.task_id}
                task={task}
                t={t}
                channelLabels={channelLabels}
                onToggle={() => handleToggleTask(task)}
                onEdit={() => handleEditTask(task)}
                onTrigger={() => handleTriggerTask(task)}
                isTriggering={triggerTaskMutation.isPending}
                onViewDetail={() => setDetailTask(task)}
                onDelete={() => setDeleteConfirmTask(task)}
                formatScheduleSummary={formatScheduleSummary}
                formatTimestamp={formatTimestamp}
              />
            ))}
          </div>
        )}
      </div>

      <TaskFormDialog
        open={formDialogOpen}
        onOpenChange={handleFormDialogClose}
        templateId={selectedTemplate}
        editTask={editTask}
      />

      <TaskDetailDialog
        task={detailTask}
        onOpenChange={(open) => { if (!open) setDetailTask(null); }}
      />

      <Dialog open={!!deleteConfirmTask} onOpenChange={(open) => { if (!open) setDeleteConfirmTask(null); }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t.settings.scheduler.deleteConfirmTitle}</DialogTitle>
            <DialogDescription>{t.settings.scheduler.deleteConfirmDescription}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteConfirmTask(null)}>{t.common.cancel}</Button>
            <Button variant="destructive" onClick={handleDeleteTask} disabled={deleteTask.isPending}>{t.common.delete}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </SettingsSection>
  );
}

function TaskCard({
  task,
  t,
  channelLabels,
  onToggle,
  onEdit,
  onTrigger,
  isTriggering,
  onViewDetail,
  onDelete,
  formatScheduleSummary,
  formatTimestamp,
}: {
  task: ScheduledTask;
  t: ReturnType<typeof useI18n>["t"];
  channelLabels: Record<string, string>;
  onToggle: () => void;
  onEdit: () => void;
  onTrigger: () => void;
  isTriggering: boolean;
  onViewDetail: () => void;
  onDelete: () => void;
  formatScheduleSummary: (task: ScheduledTask) => string;
  formatTimestamp: (ts: number | null) => string;
}) {
  const statusConfig: Record<string, { color: string; label: string }> = {
    active: { color: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400", label: t.settings.scheduler.active },
    paused: { color: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400", label: t.settings.scheduler.paused },
    completed: { color: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400", label: t.settings.scheduler.completed },
  };
  const st = statusConfig[task.status] || statusConfig.active;

  return (
    <div
      className="group rounded-lg border bg-card p-4 transition-colors hover:bg-accent/30"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0 space-y-2">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium truncate">{task.task_name}</h3>
            <span className={cn("shrink-0 inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium", st.color)}>
              {st.label}
            </span>
          </div>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span className="inline-flex items-center gap-1">
              <Clock className="size-3" />
              {formatScheduleSummary(task)}
            </span>
            <span className="inline-flex items-center gap-1">
              {task.notify_channels.map((ch) => {
                const Icon = CHANNEL_ICONS[ch] || Bell;
                return <Icon key={ch} className="size-3" />;
              })}
              {task.notify_channels.map((ch) => channelLabels[ch] || ch).join(", ")}
            </span>
          </div>

          {task.next_run_at && (
            <div className="text-[11px] text-muted-foreground">
              {t.settings.scheduler.nextRunAt}: {formatTimestamp(task.next_run_at)}
            </div>
          )}
        </div>

        <div className="flex shrink-0 items-center gap-0.5 opacity-0 transition-opacity group-hover:opacity-100">
          <Button
            variant="ghost"
            size="icon"
            className="size-8"
            onClick={onTrigger}
            disabled={isTriggering}
            title={t.settings.scheduler.triggerTask}
          >
            <Zap className={cn("size-3.5", isTriggering && "animate-pulse")} />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="size-8"
            onClick={onToggle}
            title={task.status === "active" ? t.settings.scheduler.pauseTask : t.settings.scheduler.resumeTask}
          >
            {task.status === "active" ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
          </Button>
          <Button variant="ghost" size="icon" className="size-8" onClick={onEdit} title={t.settings.scheduler.editTask}>
            <Pencil className="size-3.5" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8" onClick={onViewDetail}>
            <Eye className="size-3.5" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8 text-destructive hover:text-destructive" onClick={onDelete}>
            <Trash2 className="size-3.5" />
          </Button>
        </div>
      </div>
    </div>
  );
}

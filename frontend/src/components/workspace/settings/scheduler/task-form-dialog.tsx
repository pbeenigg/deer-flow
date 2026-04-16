"use client";

import { useState, useId, useEffect } from "react";
import { Mail, MessageCircle, Send, Bell, Clock, Calendar, Timer, Hash } from "lucide-react";
import { toast } from "sonner";

import { useI18n } from "@/core/i18n/hooks";
import { useCreateScheduledTask, useUpdateScheduledTask, useAvailableChannels, useTaskTemplates } from "@/core/scheduler/hooks";
import type { ScheduleConfig, ScheduledTask } from "@/core/scheduler/api";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

interface TaskFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  templateId?: string;
  editTask?: ScheduledTask | null;
}

const SCHEDULE_TYPES = ["once", "daily", "weekly", "interval", "cron"];
const DAYS_OF_WEEK = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];

const CHANNEL_ICONS: Record<string, React.ElementType> = {
  email: Mail,
  wechat: MessageCircle,
  telegram: Send,
  dingtalk: Bell,
};

export function TaskFormDialog({ open, onOpenChange, templateId, editTask }: TaskFormDialogProps) {
  const { t } = useI18n();
  const createTask = useCreateScheduledTask();
  const updateTask = useUpdateScheduledTask();
  const { channels } = useAvailableChannels();
  const { templates } = useTaskTemplates();

  const isEditing = !!editTask;

  const nameId = useId();
  const promptId = useId();

  const selectedTemplate = templateId ? templates.find((tpl) => tpl.id === templateId) : null;

  const [taskName, setTaskName] = useState("");
  const [taskType, setTaskType] = useState("custom_query");
  const [taskPrompt, setTaskPrompt] = useState("");
  const [scheduleType, setScheduleType] = useState("daily");
  const [hour, setHour] = useState(9);
  const [minute, setMinute] = useState(0);
  const [dayOfWeek, setDayOfWeek] = useState("monday");
  const [intervalMinutes, setIntervalMinutes] = useState(60);
  const [cronExpression, setCronExpression] = useState("0 9 * * *");
  const [timezone, setTimezone] = useState("Asia/Shanghai");
  const [selectedChannels, setSelectedChannels] = useState<string[]>([]);
  const [notifyConfig, setNotifyConfig] = useState<Record<string, Record<string, string>>>({});

  useEffect(() => {
    if (!open) return;

    if (editTask) {
      const cfg = editTask.schedule_config || {};
      setTaskName(editTask.task_name);
      setTaskType(editTask.task_type);
      setTaskPrompt(editTask.task_prompt);
      setScheduleType(editTask.schedule_type);
      setHour(cfg.hour ?? 9);
      setMinute(cfg.minute ?? 0);
      setDayOfWeek(cfg.day_of_week || "monday");
      setIntervalMinutes(cfg.minutes ?? 60);
      setCronExpression(cfg.expression || "0 9 * * *");
      setTimezone(editTask.timezone);
      setSelectedChannels(editTask.notify_channels);
      setNotifyConfig(editTask.notify_config as Record<string, Record<string, string>> || {});
    } else {
      setTaskName(selectedTemplate?.name || "");
      setTaskType(selectedTemplate?.task_type || "custom_query");
      setTaskPrompt(selectedTemplate?.task_prompt || "");
      setScheduleType(selectedTemplate?.schedule?.type || "daily");
      setHour(selectedTemplate?.schedule?.hour ?? 9);
      setMinute(selectedTemplate?.schedule?.minute ?? 0);
      setDayOfWeek(selectedTemplate?.schedule?.day_of_week || "monday");
      setSelectedChannels(selectedTemplate?.notify_channels || []);
      setNotifyConfig({});
    }
  }, [open, editTask, selectedTemplate]);

  function handleChannelToggle(channel: string) {
    setSelectedChannels((prev) =>
      prev.includes(channel) ? prev.filter((c) => c !== channel) : [...prev, channel],
    );
  }

  function updateChannelConfig(channel: string, key: string, value: string) {
    setNotifyConfig((prev) => ({
      ...prev,
      [channel]: { ...prev[channel], [key]: value },
    }));
  }

  function buildSchedule(): ScheduleConfig {
    const schedule: ScheduleConfig = { type: scheduleType };
    if (scheduleType === "once" || scheduleType === "daily") {
      schedule.hour = hour;
      schedule.minute = minute;
    } else if (scheduleType === "weekly") {
      schedule.day_of_week = dayOfWeek;
      schedule.hour = hour;
      schedule.minute = minute;
    } else if (scheduleType === "interval") {
      schedule.minutes = intervalMinutes;
    } else if (scheduleType === "cron") {
      schedule.expression = cronExpression;
    }
    return schedule;
  }

  async function handleSubmit() {
    if (!taskName.trim()) {
      toast.error(t.settings.scheduler.validationName);
      return;
    }
    if (!taskPrompt.trim()) {
      toast.error(t.settings.scheduler.validationPrompt);
      return;
    }

    const schedule = buildSchedule();

    if (isEditing && editTask) {
      updateTask.mutate(
        {
          taskId: editTask.task_id,
          data: {
            task_name: taskName,
            task_prompt: taskPrompt,
            schedule,
            timezone,
            notify_channels: selectedChannels,
            notify_config: notifyConfig,
          },
        },
        {
          onSuccess: () => {
            toast.success(t.settings.scheduler.updateSuccess);
            onOpenChange(false);
          },
        },
      );
    } else {
      createTask.mutate(
        {
          task_name: taskName,
          task_type: taskType,
          task_prompt: taskPrompt,
          schedule,
          timezone,
          notify_channels: selectedChannels,
          notify_config: notifyConfig,
        },
        {
          onSuccess: () => {
            toast.success(t.settings.scheduler.createSuccess);
            onOpenChange(false);
          },
        },
      );
    }
  }

  function renderChannelConfig(channel: string) {
    const config = notifyConfig[channel] || {};
    switch (channel) {
      case "email":
        return (
          <div className="grid grid-cols-2 gap-2">
            <Input className="col-span-2" placeholder={t.settings.scheduler.emailToEmail} value={config.to_email || ""} onChange={(e) => updateChannelConfig(channel, "to_email", e.target.value)} />
            <Input placeholder={t.settings.scheduler.emailSmtpHost} value={config.smtp_host || ""} onChange={(e) => updateChannelConfig(channel, "smtp_host", e.target.value)} />
            <Input placeholder={t.settings.scheduler.emailSmtpPort} value={config.smtp_port || ""} onChange={(e) => updateChannelConfig(channel, "smtp_port", e.target.value)} />
            <Input placeholder={t.settings.scheduler.emailSmtpUser} value={config.smtp_user || ""} onChange={(e) => updateChannelConfig(channel, "smtp_user", e.target.value)} />
            <Input type="password" placeholder={t.settings.scheduler.emailSmtpPassword} value={config.smtp_password || ""} onChange={(e) => updateChannelConfig(channel, "smtp_password", e.target.value)} />
          </div>
        );
      case "wechat":
        return (
          <Input placeholder={t.settings.scheduler.wechatWebhookUrl} value={config.webhook_url || ""} onChange={(e) => updateChannelConfig(channel, "webhook_url", e.target.value)} />
        );
      case "telegram":
        return (
          <div className="grid grid-cols-2 gap-2">
            <Input className="col-span-2" placeholder={t.settings.scheduler.telegramBotToken} type="password" value={config.bot_token || ""} onChange={(e) => updateChannelConfig(channel, "bot_token", e.target.value)} />
            <Input className="col-span-2" placeholder={t.settings.scheduler.telegramChatId} value={config.chat_id || ""} onChange={(e) => updateChannelConfig(channel, "chat_id", e.target.value)} />
          </div>
        );
      case "dingtalk":
        return (
          <Input placeholder={t.settings.scheduler.dingtalkWebhookUrl} value={config.webhook_url || ""} onChange={(e) => updateChannelConfig(channel, "webhook_url", e.target.value)} />
        );
      default:
        return null;
    }
  }

  const channelLabels: Record<string, string> = {
    email: t.settings.scheduler.channelEmail,
    wechat: t.settings.scheduler.channelWechat,
    telegram: t.settings.scheduler.channelTelegram,
    dingtalk: t.settings.scheduler.channelDingtalk,
  };

  const scheduleTypeIcons: Record<string, React.ElementType> = {
    once: Calendar,
    daily: Clock,
    weekly: Calendar,
    interval: Timer,
    cron: Hash,
  };

  const isPending = createTask.isPending || updateTask.isPending;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>{isEditing ? t.settings.scheduler.editTask : t.settings.scheduler.createTask}</DialogTitle>
          <DialogDescription>{t.settings.scheduler.description}</DialogDescription>
        </DialogHeader>
        <ScrollArea className="max-h-[60vh] pr-2">
          <div className="space-y-6">
            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.taskName}
              </div>
              <Input id={nameId} value={taskName} onChange={(e) => setTaskName(e.target.value)} placeholder={t.settings.scheduler.taskNamePlaceholder} />
              <div className="flex gap-2">
                <Select value={taskType} onValueChange={setTaskType} disabled={isEditing}>
                  <SelectTrigger className="flex-1"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="news_digest">{t.settings.scheduler.taskTypeNewsDigest}</SelectItem>
                    <SelectItem value="custom_query">{t.settings.scheduler.taskTypeCustomQuery}</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={timezone} onValueChange={setTimezone}>
                  <SelectTrigger className="w-[160px]"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Asia/Shanghai">Asia/Shanghai</SelectItem>
                    <SelectItem value="America/New_York">America/New_York</SelectItem>
                    <SelectItem value="Europe/London">Europe/London</SelectItem>
                    <SelectItem value="Asia/Tokyo">Asia/Tokyo</SelectItem>
                    <SelectItem value="UTC">UTC</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-2">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.taskPrompt}
              </div>
              <Textarea id={promptId} value={taskPrompt} onChange={(e) => setTaskPrompt(e.target.value)} placeholder={t.settings.scheduler.taskPromptPlaceholder} rows={3} className="resize-none" />
            </div>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.scheduleType}
              </div>
              <div className="grid grid-cols-5 gap-1.5">
                {SCHEDULE_TYPES.map((type) => {
                  const Icon = scheduleTypeIcons[type] || Clock;
                  const isActive = scheduleType === type;
                  const labelKey = `schedule${type.charAt(0).toUpperCase() + type.slice(1)}` as keyof typeof t.settings.scheduler;
                  return (
                    <button
                      key={type}
                      type="button"
                      onClick={() => setScheduleType(type)}
                      className={cn(
                        "flex flex-col items-center gap-1 rounded-lg border px-2 py-2.5 text-xs font-medium transition-colors",
                        isActive
                          ? "border-primary bg-primary/10 text-primary"
                          : "border-border text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                      )}
                    >
                      <Icon className="size-4" />
                      {t.settings.scheduler[labelKey] || type}
                    </button>
                  );
                })}
              </div>

              {(scheduleType === "once" || scheduleType === "daily" || scheduleType === "weekly") && (
                <div className="flex gap-2">
                  <div className="flex-1">
                    <Input type="number" min={0} max={23} value={hour} onChange={(e) => setHour(Number(e.target.value))} className="text-center" />
                    <div className="mt-1 text-center text-[10px] text-muted-foreground">{t.settings.scheduler.hour}</div>
                  </div>
                  <div className="flex items-center text-xl text-muted-foreground font-light">:</div>
                  <div className="flex-1">
                    <Input type="number" min={0} max={59} value={minute} onChange={(e) => setMinute(Number(e.target.value))} className="text-center" />
                    <div className="mt-1 text-center text-[10px] text-muted-foreground">{t.settings.scheduler.minute}</div>
                  </div>
                  {scheduleType === "weekly" && (
                    <div className="flex-[2]">
                      <Select value={dayOfWeek} onValueChange={setDayOfWeek}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          {DAYS_OF_WEEK.map((day) => (
                            <SelectItem key={day} value={day}>
                              {t.settings.scheduler.daysOfWeek[day as keyof typeof t.settings.scheduler.daysOfWeek]}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}
                </div>
              )}

              {scheduleType === "interval" && (
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground whitespace-nowrap">{t.settings.scheduler.scheduleInterval}</span>
                  <Input type="number" min={1} value={intervalMinutes} onChange={(e) => setIntervalMinutes(Number(e.target.value))} className="w-24 text-center" />
                  <span className="text-sm text-muted-foreground">{t.settings.scheduler.minute}</span>
                </div>
              )}

              {scheduleType === "cron" && (
                <div className="space-y-1">
                  <Input value={cronExpression} onChange={(e) => setCronExpression(e.target.value)} placeholder="0 9 * * 1-5" className="font-mono" />
                  <div className="text-[10px] text-muted-foreground">min hour day month dow</div>
                </div>
              )}
            </div>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {t.settings.scheduler.notifyChannels}
              </div>
              <div className="grid grid-cols-2 gap-2">
                {channels.map((channel) => {
                  const Icon = CHANNEL_ICONS[channel] || Bell;
                  const isSelected = selectedChannels.includes(channel);
                  return (
                    <button
                      key={channel}
                      type="button"
                      onClick={() => handleChannelToggle(channel)}
                      className={cn(
                        "flex items-center gap-2.5 rounded-lg border px-3 py-2.5 text-sm transition-colors",
                        isSelected
                          ? "border-primary bg-primary/10 text-primary"
                          : "border-border text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                      )}
                    >
                      <Icon className="size-4 shrink-0" />
                      <span className="font-medium">{channelLabels[channel] || channel}</span>
                      {isSelected && (
                        <span className="ml-auto size-2 rounded-full bg-primary" />
                      )}
                    </button>
                  );
                })}
              </div>

              {selectedChannels.map((channel) => (
                <div key={channel} className="rounded-lg border bg-muted/30 p-3 space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    {(() => { const Icon = CHANNEL_ICONS[channel] || Bell; return <Icon className="size-3.5" />; })()}
                    {channelLabels[channel] || channel}
                  </div>
                  {renderChannelConfig(channel)}
                </div>
              ))}
            </div>
          </div>
        </ScrollArea>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>{t.common.cancel}</Button>
          <Button onClick={handleSubmit} disabled={isPending}>
            {isEditing ? t.common.save : t.common.create}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

import { getBackendBaseURL } from "@/core/config";

export interface ScheduleConfig {
  type: string;
  hour?: number | null;
  minute?: number | null;
  day_of_week?: string | null;
  minutes?: number | null;
  expression?: string | null;
}

export interface ScheduledTask {
  task_id: string;
  task_name: string;
  task_type: string;
  task_prompt: string;
  schedule_type: string;
  schedule_config: ScheduleConfig;
  timezone: string;
  notify_channels: string[];
  notify_config: Record<string, Record<string, string>>;
  status: string;
  last_run_at: number | null;
  next_run_at: number | null;
  thread_id: string | null;
  created_at: number;
  updated_at: number;
}

export interface TaskExecution {
  id: string;
  task_id: string;
  started_at: number;
  finished_at: number | null;
  status: string;
  error_message: string | null;
  result_content: string | null;
  notify_status: Record<string, Record<string, string>> | null;
  created_at: number;
}

export interface TaskTemplate {
  id: string;
  name: string;
  description: string;
  task_type: string;
  task_prompt: string;
  schedule: ScheduleConfig;
  notify_channels: string[];
}

export interface SchedulerStatus {
  enabled: boolean;
  running: boolean;
  active_tasks: number;
}

export interface CreateTaskRequest {
  task_name: string;
  task_type: string;
  task_prompt: string;
  schedule: ScheduleConfig;
  timezone?: string;
  notify_channels: string[];
  notify_config: Record<string, Record<string, string>>;
  user_id?: string;
  thread_id?: string | null;
}

export interface UpdateTaskRequest {
  task_name?: string;
  task_prompt?: string;
  schedule?: ScheduleConfig;
  timezone?: string;
  notify_channels?: string[];
  notify_config?: Record<string, Record<string, string>>;
}

const BASE = `${getBackendBaseURL()}/api/scheduled-tasks`;

export async function loadScheduledTasks(): Promise<ScheduledTask[]> {
  const response = await fetch(BASE);
  if (!response.ok) throw new Error("Failed to load scheduled tasks");
  return response.json();
}

export async function loadScheduledTask(taskId: string): Promise<ScheduledTask> {
  const response = await fetch(`${BASE}/${taskId}`);
  if (!response.ok) throw new Error("Failed to load scheduled task");
  return response.json();
}

export async function createScheduledTask(data: CreateTaskRequest): Promise<ScheduledTask> {
  const response = await fetch(BASE, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  if (!response.ok) throw new Error("Failed to create scheduled task");
  return response.json();
}

export async function updateScheduledTask(taskId: string, data: UpdateTaskRequest): Promise<ScheduledTask> {
  const response = await fetch(`${BASE}/${taskId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  if (!response.ok) throw new Error("Failed to update scheduled task");
  return response.json();
}

export async function updateTaskStatus(taskId: string, status: string): Promise<ScheduledTask> {
  const response = await fetch(`${BASE}/${taskId}/status`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status }),
  });
  if (!response.ok) throw new Error("Failed to update task status");
  return response.json();
}

export async function deleteScheduledTask(taskId: string): Promise<void> {
  const response = await fetch(`${BASE}/${taskId}`, { method: "DELETE" });
  if (!response.ok) throw new Error("Failed to delete scheduled task");
}

export async function loadTaskExecutions(taskId: string): Promise<TaskExecution[]> {
  const response = await fetch(`${BASE}/${taskId}/executions`);
  if (!response.ok) throw new Error("Failed to load task executions");
  return response.json();
}

export async function triggerTask(taskId: string): Promise<TaskExecution> {
  const response = await fetch(`${BASE}/${taskId}/trigger`, {
    method: "POST",
  });
  if (!response.ok) throw new Error("Failed to trigger task");
  return response.json();
}

export async function loadSchedulerStatus(): Promise<SchedulerStatus> {
  const response = await fetch(`${BASE}/status`);
  if (!response.ok) throw new Error("Failed to load scheduler status");
  return response.json();
}

export async function loadAvailableChannels(): Promise<{ channels: string[] }> {
  const response = await fetch(`${BASE}/channels`);
  if (!response.ok) throw new Error("Failed to load available channels");
  return response.json();
}

export async function loadTaskTemplates(): Promise<TaskTemplate[]> {
  const response = await fetch(`${BASE}/templates/list`);
  if (!response.ok) throw new Error("Failed to load task templates");
  return response.json();
}

export async function testNotifyChannel(channel: string, config: Record<string, string>, testMessage: string): Promise<Record<string, Record<string, string>>> {
  const response = await fetch(`${BASE}/notify/test`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ channel, config, test_message: testMessage }),
  });
  if (!response.ok) throw new Error("Failed to test notification");
  return response.json();
}

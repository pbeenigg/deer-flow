import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  createScheduledTask,
  deleteScheduledTask,
  loadAvailableChannels,
  loadScheduledTask,
  loadScheduledTasks,
  loadSchedulerStatus,
  loadTaskExecutions,
  loadTaskTemplates,
  testNotifyChannel,
  triggerTask,
  updateScheduledTask,
  updateTaskStatus,
  type CreateTaskRequest,
  type ScheduledTask,
  type TaskExecution,
  type TaskTemplate,
  type SchedulerStatus,
  type UpdateTaskRequest,
} from "./api";

export function useScheduledTasks() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["scheduledTasks"],
    queryFn: () => loadScheduledTasks(),
    refetchOnWindowFocus: false,
  });
  return { tasks: data ?? [], isLoading, error };
}

export function useScheduledTask(taskId: string | null) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["scheduledTask", taskId],
    queryFn: () => loadScheduledTask(taskId!),
    enabled: !!taskId,
    refetchOnWindowFocus: false,
  });
  return { task: data, isLoading, error };
}

export function useTaskExecutions(taskId: string | null) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["taskExecutions", taskId],
    queryFn: () => loadTaskExecutions(taskId!),
    enabled: !!taskId,
    refetchOnWindowFocus: false,
    refetchInterval: (query) => {
      const executions = query.state.data ?? [];
      const hasRunning = executions.some((e) => e.status === "running");
      return hasRunning ? 3000 : false;
    },
  });
  return { executions: data ?? [], isLoading, error };
}

export function useSchedulerStatus() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["schedulerStatus"],
    queryFn: () => loadSchedulerStatus(),
    refetchOnWindowFocus: false,
  });
  return { status: data as SchedulerStatus | undefined, isLoading, error };
}

export function useAvailableChannels() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["availableChannels"],
    queryFn: () => loadAvailableChannels(),
    refetchOnWindowFocus: false,
  });
  return { channels: data?.channels ?? [], isLoading, error };
}

export function useTaskTemplates() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["taskTemplates"],
    queryFn: () => loadTaskTemplates(),
    refetchOnWindowFocus: false,
  });
  return { templates: data ?? [], isLoading, error };
}

export function useCreateScheduledTask() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateTaskRequest) => createScheduledTask(data),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["scheduledTasks"] });
      void queryClient.invalidateQueries({ queryKey: ["schedulerStatus"] });
    },
  });
}

export function useUpdateScheduledTask() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ taskId, data }: { taskId: string; data: UpdateTaskRequest }) =>
      updateScheduledTask(taskId, data),
    onSuccess: (_, { taskId }) => {
      void queryClient.invalidateQueries({ queryKey: ["scheduledTasks"] });
      void queryClient.invalidateQueries({ queryKey: ["scheduledTask", taskId] });
    },
  });
}

export function useUpdateTaskStatus() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ taskId, status }: { taskId: string; status: string }) =>
      updateTaskStatus(taskId, status),
    onSuccess: (_, { taskId }) => {
      void queryClient.invalidateQueries({ queryKey: ["scheduledTasks"] });
      void queryClient.invalidateQueries({ queryKey: ["scheduledTask", taskId] });
      void queryClient.invalidateQueries({ queryKey: ["schedulerStatus"] });
    },
  });
}

export function useDeleteScheduledTask() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (taskId: string) => deleteScheduledTask(taskId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["scheduledTasks"] });
      void queryClient.invalidateQueries({ queryKey: ["schedulerStatus"] });
    },
  });
}

export function useTestNotifyChannel() {
  return useMutation({
    mutationFn: ({ channel, config, testMessage }: { channel: string; config: Record<string, string>; testMessage: string }) =>
      testNotifyChannel(channel, config, testMessage),
  });
}

export function useTriggerTask() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (taskId: string) => triggerTask(taskId),
    onSuccess: (_, taskId) => {
      void queryClient.invalidateQueries({ queryKey: ["taskExecutions", taskId] });
      void queryClient.invalidateQueries({ queryKey: ["scheduledTasks"] });
    },
  });
}

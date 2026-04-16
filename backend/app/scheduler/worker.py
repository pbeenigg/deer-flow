"""TaskWorker — executes scheduled tasks by invoking the DeerFlow agent.

Reuses the existing LangGraph Server API (via langgraph_sdk) to run
the agent, then dispatches results through the notification service.
This mirrors the ChannelManager's approach for consistency.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from app.scheduler.notify import NotifyService
from app.scheduler.store import ScheduledTaskStore

logger = logging.getLogger(__name__)

DEFAULT_LANGGRAPH_URL = "http://localhost:2024"
DEFAULT_ASSISTANT_ID = "lead_agent"


class TaskWorker:
    """Executes scheduled tasks by calling the DeerFlow agent.

    Uses langgraph_sdk to invoke runs.wait on the LangGraph Server,
    then sends the result through configured notification channels.
    """

    def __init__(
        self,
        store: ScheduledTaskStore,
        *,
        langgraph_url: str = DEFAULT_LANGGRAPH_URL,
    ) -> None:
        self._store = store
        self._langgraph_url = langgraph_url
        self._notify = NotifyService()
        self._client = None
        self._semaphore: asyncio.Semaphore | None = None
        self._running = False

    def _get_client(self):
        """Return the langgraph_sdk async client, creating it on first use."""
        if self._client is None:
            from langgraph_sdk import get_client

            self._client = get_client(url=self._langgraph_url)
        return self._client

    async def start(self, *, max_concurrency: int = 3) -> None:
        """Start the worker."""
        if self._running:
            return
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._running = True
        logger.info("TaskWorker started (max_concurrency=%d)", max_concurrency)

    async def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        logger.info("TaskWorker stopped")

    async def execute_task(self, task: dict[str, Any]) -> None:
        """Execute a scheduled task.

        1. Create an execution record
        2. Find or create a thread for the task
        3. Invoke the agent via runs.wait
        4. Send the result through notification channels
        5. Update the execution record
        """
        if not self._running:
            logger.warning("TaskWorker not running, skipping task %s", task.get("id"))
            return

        async with self._semaphore or asyncio.Semaphore(1):
            task_id = task["id"]
            execution = self._store.create_execution(task_id)
            execution_id = execution["id"]

            logger.info("Executing task %s (execution %s)", task_id, execution_id)

            try:
                result_text = await self._run_agent(task)

                notify_status = await self._notify.send(
                    channels=task.get("notify_channels", []),
                    content=result_text,
                    config=task.get("notify_config", {}),
                    task_name=task.get("task_name", "DeerFlow 定时推送"),
                )

                self._store.update_execution(
                    execution_id,
                    status="success",
                    finished_at=time.time(),
                    result_content=result_text,
                    notify_status=notify_status,
                )
                logger.info("Task %s executed successfully", task_id)

            except Exception as e:
                logger.exception("Task %s execution failed", task_id)
                self._store.update_execution(
                    execution_id,
                    status="failed",
                    finished_at=time.time(),
                    error_message=str(e),
                )

    async def _run_agent(self, task: dict[str, Any]) -> str:
        """Run the agent and return the response text."""
        client = self._get_client()

        thread_id = task.get("thread_id")
        if not thread_id:
            thread = await client.threads.create()
            thread_id = thread["thread_id"]
            self._store.update_task(task["id"], thread_id=thread_id)

        result = await client.runs.wait(
            thread_id,
            DEFAULT_ASSISTANT_ID,
            input={"messages": [{"role": "human", "content": task["task_prompt"]}]},
            config={"recursion_limit": 100},
            context={
                "thinking_enabled": True,
                "is_plan_mode": False,
                "subagent_enabled": False,
            },
        )

        return self._extract_response_text(result)

    @staticmethod
    def _extract_response_text(result: dict | list) -> str:
        """Extract the last AI message text from a LangGraph runs.wait result."""
        if isinstance(result, list):
            messages = result
        elif isinstance(result, dict):
            messages = result.get("messages", [])
        else:
            return ""

        for msg in reversed(messages):
            if not isinstance(msg, dict):
                continue
            msg_type = msg.get("type")
            if msg_type == "human":
                break
            if msg_type == "tool" and msg.get("name") == "ask_clarification":
                content = msg.get("content", "")
                if isinstance(content, str) and content:
                    return content
            if msg_type == "ai":
                content = msg.get("content", "")
                if isinstance(content, str) and content:
                    return content
                if isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            parts.append(block.get("text", ""))
                        elif isinstance(block, str):
                            parts.append(block)
                    text = "".join(parts)
                    if text:
                        return text
        return ""

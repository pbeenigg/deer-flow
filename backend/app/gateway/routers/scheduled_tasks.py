"""Gateway router for scheduled task management."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.scheduler.scheduler import compute_next_run_time
from app.scheduler.store import ScheduledTaskStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scheduled-tasks", tags=["scheduled-tasks"])


def _get_store(request: Request) -> ScheduledTaskStore:
    """Get the ScheduledTaskStore from the scheduler service."""
    from app.scheduler.service import get_scheduler_service

    service = get_scheduler_service()
    if service is None:
        raise HTTPException(status_code=503, detail="Scheduler service is not available")
    return service.store


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ScheduleConfig(BaseModel):
    type: str = Field(description="Schedule type: once, daily, weekly, interval, cron")
    hour: int | None = Field(default=None, description="Hour (0-23)")
    minute: int | None = Field(default=None, description="Minute (0-59)")
    day_of_week: str | None = Field(default=None, description="Day of week (for weekly)")
    minutes: int | None = Field(default=None, description="Interval in minutes (for interval)")
    expression: str | None = Field(default=None, description="Cron expression (for cron)")


class TaskCreateRequest(BaseModel):
    task_name: str = Field(description="Task name")
    task_type: str = Field(default="custom_query", description="Task type: news_digest, custom_query")
    task_prompt: str = Field(description="Prompt to execute")
    schedule: ScheduleConfig = Field(description="Schedule configuration")
    timezone: str = Field(default="Asia/Shanghai", description="Timezone")
    notify_channels: list[str] = Field(default_factory=lambda: ["email"], description="Notification channels")
    notify_config: dict[str, Any] = Field(default_factory=dict, description="Channel-specific config")
    user_id: str = Field(default="default", description="User ID")
    thread_id: str | None = Field(default=None, description="Existing thread ID to reuse")


class TaskUpdateRequest(BaseModel):
    task_name: str | None = None
    task_prompt: str | None = None
    schedule: ScheduleConfig | None = None
    timezone: str | None = None
    notify_channels: list[str] | None = None
    notify_config: dict[str, Any] | None = None


class TaskStatusRequest(BaseModel):
    status: str = Field(description="New status: active, paused")


class TaskResponse(BaseModel):
    task_id: str
    task_name: str
    task_type: str
    task_prompt: str
    schedule_type: str
    schedule_config: dict[str, Any]
    timezone: str
    notify_channels: list[str]
    notify_config: dict[str, Any]
    status: str
    last_run_at: float | None = None
    next_run_at: float | None = None
    thread_id: str | None = None
    created_at: float
    updated_at: float


class ExecutionResponse(BaseModel):
    id: str
    task_id: str
    started_at: float
    finished_at: float | None = None
    status: str
    error_message: str | None = None
    result_content: str | None = None
    notify_status: dict[str, Any] | None = None
    created_at: float


class NotifyTestRequest(BaseModel):
    channel: str = Field(description="Channel to test")
    config: dict[str, Any] = Field(description="Channel config")
    test_message: str = Field(default="This is a test message from DeerFlow", description="Test message content")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task_to_response(task: dict[str, Any]) -> TaskResponse:
    return TaskResponse(
        task_id=task["id"],
        task_name=task.get("task_name", ""),
        task_type=task.get("task_type", ""),
        task_prompt=task.get("task_prompt", ""),
        schedule_type=task.get("schedule_type", ""),
        schedule_config=task.get("schedule_config", {}),
        timezone=task.get("timezone", "Asia/Shanghai"),
        notify_channels=task.get("notify_channels", []),
        notify_config=task.get("notify_config", {}),
        status=task.get("status", ""),
        last_run_at=task.get("last_run_at"),
        next_run_at=task.get("next_run_at"),
        thread_id=task.get("thread_id"),
        created_at=task.get("created_at", 0),
        updated_at=task.get("updated_at", 0),
    )


def _execution_to_response(execution: dict[str, Any]) -> ExecutionResponse:
    return ExecutionResponse(
        id=execution["id"],
        task_id=execution.get("task_id", ""),
        started_at=execution.get("started_at", 0),
        finished_at=execution.get("finished_at"),
        status=execution.get("status", ""),
        error_message=execution.get("error_message"),
        result_content=execution.get("result_content"),
        notify_status=execution.get("notify_status"),
        created_at=execution.get("created_at", 0),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("", response_model=TaskResponse)
async def create_task(body: TaskCreateRequest, request: Request) -> TaskResponse:
    """Create a new scheduled task."""
    store = _get_store(request)

    schedule_dict = body.schedule.model_dump(exclude_none=True)
    schedule_type = schedule_dict.pop("type")

    task = store.create_task(
        user_id=body.user_id,
        task_name=body.task_name,
        task_type=body.task_type,
        task_prompt=body.task_prompt,
        schedule_type=schedule_type,
        schedule_config=schedule_dict,
        timezone=body.timezone,
        notify_channels=body.notify_channels,
        notify_config=body.notify_config,
        thread_id=body.thread_id,
    )

    next_run = compute_next_run_time(
        schedule_type,
        schedule_dict,
        body.timezone,
    )
    if next_run is not None:
        store.update_task(task["id"], next_run_at=next_run)
        task["next_run_at"] = next_run
    else:
        store.update_task(task["id"], status="completed")
        task["status"] = "completed"

    return _task_to_response(store.get_task(task["id"]) or task)


@router.get("", response_model=list[TaskResponse])
async def list_tasks(
    request: Request,
    user_id: str | None = None,
    status: str | None = None,
) -> list[TaskResponse]:
    """List scheduled tasks, optionally filtered."""
    store = _get_store(request)
    tasks = store.list_tasks(user_id=user_id, status=status)
    return [_task_to_response(t) for t in tasks]


@router.get("/status")
async def get_scheduler_status(request: Request) -> dict:
    """Get the scheduler service status."""
    from app.scheduler.service import get_scheduler_service

    service = get_scheduler_service()
    if service is None:
        return {"enabled": False, "running": False, "active_tasks": 0}
    return service.get_status()


@router.get("/channels")
async def get_available_channels() -> dict:
    """Get available notification channels."""
    from app.scheduler.notify import NotifyService

    return {"channels": NotifyService.get_available_channels()}


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request) -> TaskResponse:
    """Get a scheduled task by ID."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return _task_to_response(task)


@router.put("/{task_id}", response_model=TaskResponse)
async def update_task(task_id: str, body: TaskUpdateRequest, request: Request) -> TaskResponse:
    """Update a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    updates: dict[str, Any] = {}
    if body.task_name is not None:
        updates["task_name"] = body.task_name
    if body.task_prompt is not None:
        updates["task_prompt"] = body.task_prompt
    if body.timezone is not None:
        updates["timezone"] = body.timezone
    if body.notify_channels is not None:
        updates["notify_channels"] = body.notify_channels
    if body.notify_config is not None:
        updates["notify_config"] = body.notify_config

    if body.schedule is not None:
        schedule_dict = body.schedule.model_dump(exclude_none=True)
        schedule_type = schedule_dict.pop("type")
        updates["schedule_type"] = schedule_type
        updates["schedule_config"] = schedule_dict

        next_run = compute_next_run_time(
            schedule_type,
            schedule_dict,
            body.timezone or task.get("timezone", "Asia/Shanghai"),
        )
        updates["next_run_at"] = next_run
        if next_run is None and task.get("status") == "active":
            updates["status"] = "completed"

    if updates:
        store.update_task(task_id, **updates)

    return _task_to_response(store.get_task(task_id) or task)


@router.patch("/{task_id}/status", response_model=TaskResponse)
async def update_task_status(task_id: str, body: TaskStatusRequest, request: Request) -> TaskResponse:
    """Pause or resume a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    if body.status not in ("active", "paused"):
        raise HTTPException(status_code=400, detail="Status must be 'active' or 'paused'")

    updates: dict[str, Any] = {"status": body.status}

    if body.status == "active" and task.get("next_run_at") is None:
        next_run = compute_next_run_time(
            task.get("schedule_type", ""),
            task.get("schedule_config", {}),
            task.get("timezone", "Asia/Shanghai"),
        )
        updates["next_run_at"] = next_run
        if next_run is None:
            updates["status"] = "completed"

    store.update_task(task_id, **updates)
    return _task_to_response(store.get_task(task_id) or task)


@router.delete("/{task_id}")
async def delete_task(task_id: str, request: Request) -> dict:
    """Delete a scheduled task (soft delete)."""
    store = _get_store(request)
    success = store.delete_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return {"success": True, "message": f"Task {task_id} deleted"}


@router.post("/{task_id}/trigger", response_model=ExecutionResponse)
async def trigger_task(task_id: str, request: Request) -> ExecutionResponse:
    """Manually trigger a scheduled task for immediate execution."""
    from app.scheduler.service import get_scheduler_service

    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    if task.get("status") == "deleted":
        raise HTTPException(status_code=400, detail="Cannot trigger a deleted task")

    running_executions = store.list_executions(task_id, limit=10)
    if any(e.get("status") == "running" for e in running_executions):
        raise HTTPException(status_code=409, detail="Task is already running, please wait for it to complete")

    service = get_scheduler_service()
    if service is None:
        raise HTTPException(status_code=503, detail="Scheduler service is not available")

    execution = store.create_execution(task_id)

    asyncio.create_task(service.worker.execute_task(task, execution_id=execution["id"]))

    return _execution_to_response(execution)


@router.get("/{task_id}/executions", response_model=list[ExecutionResponse])
async def list_executions(
    task_id: str,
    request: Request,
    limit: int = 20,
    offset: int = 0,
) -> list[ExecutionResponse]:
    """Get execution history for a scheduled task."""
    store = _get_store(request)
    task = store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    executions = store.list_executions(task_id, limit=limit, offset=offset)
    return [_execution_to_response(e) for e in executions]


@router.post("/notify/test")
async def test_notify(body: NotifyTestRequest) -> dict:
    """Test a notification channel configuration."""
    from app.scheduler.notify import NotifyService

    service = NotifyService()
    results = await service.send(
        channels=[body.channel],
        content=body.test_message,
        config={body.channel: body.config},
    )
    return results


@router.get("/templates/list")
async def list_task_templates() -> list[dict[str, Any]]:
    """List available task templates."""
    return TASK_TEMPLATES


TASK_TEMPLATES: list[dict[str, Any]] = [
    {
        "id": "daily-tech-news",
        "name": "🔬 每日科技洞察",
        "description": "每日精选科技领域核心动态，深度解读行业信号",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位资深科技分析师，请搜索并总结今日科技领域的重要动态。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，每条信息必须附带来源链接\n"
            "- 严禁编造、推测或拼凑任何不存在的事件、数据或产品\n"
            "- 如搜索结果不足以支撑某条内容，宁可不写也不虚构\n\n"
            "【输出格式】\n"
            "## 🤖 人工智能\n"
            "- **[标题]** 一句话摘要 + 潜在影响判断 [citation:来源](URL)\n\n"
            "## 🌐 互联网与平台\n"
            "- **[标题]** 一句话摘要 + 潜在影响判断 [citation:来源](URL)\n\n"
            "## 💻 硬件与芯片\n"
            "- **[标题]** 一句话摘要 + 潜在影响判断 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "每个领域精选2-3条最具信号价值的事件，"
            "侧重对技术演进和商业格局有实质影响的内容，"
            "过滤掉营销噪音和重复报道。总字数600字以内。"
        ),
        "schedule": {"type": "daily", "hour": 9, "minute": 0},
        "notify_channels": ["wechat"],
    },
    {
        "id": "weekly-industry-report",
        "name": "📊 周度行业研报",
        "description": "每周一深度剖析行业格局变迁与趋势信号",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位行业研究分析师，请搜索并生成本周行业深度分析报告。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，所有事件、数据、融资金额必须附带来源链接\n"
            "- 严禁编造任何事件、公司名称、融资金额或技术细节\n"
            "- 趋势研判必须有至少2条真实事件作为依据，不可空泛推断\n\n"
            "【输出格式】\n"
            "## 📌 本周核心事件（5条）\n"
            "每条包含：标题 | 事件概述 | 影响评级(⭐~⭐⭐⭐) | 影响分析 [citation:来源](URL)\n\n"
            "## 📈 技术趋势研判\n"
            "识别本周浮现的技术趋势，分析其成熟度和落地前景（每条趋势至少引用1个真实事件支撑）\n\n"
            "## 💰 资本与融资\n"
            "本周重点融资事件及资本流向分析 [citation:来源](URL)\n\n"
            "## 🔮 下周展望\n"
            "基于本周信号对下周可能的重要事件做出预判\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "要求：观点鲜明、数据支撑、避免泛泛而谈，总字数1000字以内。"
        ),
        "schedule": {"type": "weekly", "day_of_week": "monday", "hour": 9, "minute": 0},
        "notify_channels": ["email"],
    },
    {
        "id": "daily-ai-progress",
        "name": "🧠 AI前沿日报",
        "description": "每日追踪AI领域最前沿的模型、论文与开源动态",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位AI领域技术研究员，请搜索并总结今日AI领域最新进展。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，每条进展必须附带来源链接\n"
            "- 论文必须提供真实标题、作者/机构及arXiv链接或期刊链接\n"
            "- 开源项目必须提供真实GitHub链接，严禁编造项目名或星标数据\n"
            "- 如搜索不到足够真实信息，宁缺毋滥，不凑数不编造\n\n"
            "【输出格式】\n"
            "## 🏗️ 模型与架构\n"
            "新模型发布、架构创新或能力突破（含关键指标对比） [citation:来源](URL)\n\n"
            "## 📄 重要论文\n"
            "精选1-2篇高影响力论文：论文标题 | 核心贡献 | 与现有方法的差异 | [arXiv链接](URL)\n\n"
            "## 🔧 开源生态\n"
            "值得关注的开源项目更新或新发布 [GitHub链接](URL)\n\n"
            "## 🏥 落地应用\n"
            "AI在垂直领域的突破性应用案例 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "聚焦真正有技术增量的内容，忽略微调套壳和营销包装，总字数500字以内。"
        ),
        "schedule": {"type": "daily", "hour": 8, "minute": 30},
        "notify_channels": ["telegram"],
    },
    {
        "id": "daily-sci-frontier",
        "name": "🔭 科研前沿速递",
        "description": "每日追踪全球顶级期刊与预印本的重磅研究",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位跨学科科研分析师，请搜索并总结今日全球科研领域的重磅进展。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，每项研究必须附带论文链接或DOI\n"
            "- 论文标题、作者、期刊名称必须与实际发表信息一致，严禁编造\n"
            "- 如无法找到论文原始链接，至少提供期刊名称+发表日期以便查证\n"
            "- 宁可少报一项进展，也不编造不存在的论文\n\n"
            "【输出格式】\n"
            "## 🔬 重大突破\n"
            "精选1-2项最具突破性的研究成果：\n"
            "- **论文标题** | 期刊/预印本来源 | [链接](URL)\n"
            "- 核心发现（一句话）\n"
            "- 为什么重要（对领域的推动意义）\n\n"
            "## 📊 方法与工具\n"
            "新实验方法、分析工具或数据集的发布 [citation:来源](URL)\n\n"
            "## 🏛️ 资金与政策\n"
            "重大科研基金动向或科技政策变化 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "覆盖领域：物理、化学、材料、计算机科学、环境科学等。"
            "侧重Nature/Science/Cell/PRL等顶刊及arXiv热门预印本，总字数500字以内。"
        ),
        "schedule": {"type": "daily", "hour": 7, "minute": 30},
        "notify_channels": ["email"],
    },
    {
        "id": "weekly-med-literature",
        "name": "💊 医学文献精要",
        "description": "每周精选医学领域高影响力文献，提炼临床与科研价值",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位医学文献审稿专家，请搜索并总结本周医学领域的重要文献。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，每篇文献必须附带DOI或期刊原文链接\n"
            "- 论文标题、期刊名称、DOI必须与PubMed/期刊官网信息完全一致，严禁编造\n"
            "- 临床试验数据必须与原始文献一致，不得篡改或近似估算\n"
            "- 如无法确认某篇文献的真实性，宁可不收录也不冒险编造\n\n"
            "【输出格式】\n"
            "## 🏆 本周必读（2-3篇）\n"
            "每篇包含：\n"
            "- 📋 **论文标题** | 期刊 | [DOI链接](https://doi.org/xxx)\n"
            "- 🎯 研究问题\n"
            "- 🔑 核心结论\n"
            "- 💡 临床/科研启示\n\n"
            "## 🧬 基础研究进展\n"
            "分子机制、通路发现、靶点验证类研究亮点 [citation:来源](URL)\n\n"
            "## 💉 临床试验动态\n"
            "关键临床试验结果或重大试验启动 [citation:来源](URL)\n\n"
            "## 📋 指南与共识\n"
            "新发布或更新的临床指南和专家共识 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "侧重NEJM/Lancet/JAMA/BMJ等顶级医学期刊，"
            "关注肿瘤、心血管、神经科学、免疫学等核心领域，总字数800字以内。"
        ),
        "schedule": {"type": "weekly", "day_of_week": "wednesday", "hour": 8, "minute": 0},
        "notify_channels": ["email"],
    },
    {
        "id": "daily-biopharma-radar",
        "name": "🧬 生物医药雷达",
        "description": "每日追踪生物医药研发管线、审评审批与产业动态",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位生物医药行业分析师，请搜索并总结今日生物医药领域的动态。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，所有审批、管线、交易信息必须附带来源链接\n"
            "- 药品名称、适应症、临床阶段必须与FDA/NMPA/EMA官方信息一致，严禁编造\n"
            "- 交易金额、合作方名称必须与公开报道一致，不得近似估算\n"
            "- 如无法核实某条动态的真实性，宁可不收录也不编造\n\n"
            "【输出格式】\n"
            "## 💊 审评审批\n"
            "FDA/NMPA/EMA的新药批准、突破性疗法认定或审评动态 [citation:来源](URL)\n\n"
            "## 🔬 研发管线进展\n"
            "重要临床阶段推进（含适应症、阶段、关键数据） [citation:来源](URL)\n\n"
            "## 🤝 交易与合作\n"
            "重大License-in/out、并购、战略合作事件 [citation:来源](URL)\n\n"
            "## 📈 产业趋势\n"
            "技术平台突破、行业报告或监管政策变化 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "关注领域：创新药、抗体药物、细胞与基因治疗、mRNA、ADC等。"
            "侧重对管线价值和竞争格局有实质影响的事件，总字数500字以内。"
        ),
        "schedule": {"type": "daily", "hour": 12, "minute": 0},
        "notify_channels": ["wechat"],
    },
    {
        "id": "weekly-clinical-trial",
        "name": "🏥 临床试验周报",
        "description": "每周汇总全球临床试验关键进展与结果披露",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位临床试验研究分析师，请搜索并总结本周全球临床试验领域的关键动态。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，所有试验信息必须附带来源链接\n"
            "- 试验名称、NCT编号、主要终点数据必须与ClinicalTrials.gov或企业公告一致\n"
            "- 安全性事件必须与公开报道一致，严禁编造或夸大\n"
            "- 如无法核实某项试验结果的真实性，宁可不收录也不编造\n\n"
            "【输出格式】\n"
            "## 🎯 重磅结果披露\n"
            "本周公布的重大临床试验结果 [citation:来源](URL)\n"
            "含：试验名称 | NCT编号 | 阶段 | 主要终点 | 关键数据\n\n"
            "## 🆕 重要试验启动\n"
            "新启动的值得关注的III期或关键性临床试验 [citation:来源](URL)\n\n"
            "## ⚠️ 安全性信号\n"
            "临床试验中的安全性警告或试验暂停事件 [citation:来源](URL)\n\n"
            "## 📊 研究方法学\n"
            "临床试验设计创新或方法学进展 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "数据来源侧重ClinicalTrials.gov、EU Clinical Trials Register及企业公告。"
            "关注肿瘤、罕见病、慢性病等核心治疗领域，总字数600字以内。"
        ),
        "schedule": {"type": "weekly", "day_of_week": "friday", "hour": 17, "minute": 0},
        "notify_channels": ["email"],
    },
    {
        "id": "daily-med-ai",
        "name": "⚕️ 医学AI前沿",
        "description": "每日追踪AI在医学影像、诊断、药物研发中的最新应用",
        "task_type": "news_digest",
        "task_prompt": (
            "你是一位医学AI交叉领域研究员，请搜索并总结今日AI在医学领域的最新应用进展。\n\n"
            "【核心原则】\n"
            "- 必须基于真实搜索结果撰写，每条进展必须附带来源链接（论文链接、新闻原文链接或官方公告链接）\n"
            "- 性能指标（AUC、灵敏度、特异度等）必须与原始论文/报告一致，严禁编造数据\n"
            "- 论文必须提供真实标题、作者机构及arXiv/期刊链接\n"
            "- 审批动态必须与FDA/CE/NMPA官方信息一致\n"
            "- 如搜索结果不足以支撑某条内容，宁可不写也不虚构\n"
            "- 严禁输出无来源链接的孤立信息，每条内容都必须可追溯可查证\n\n"
            "【输出格式】\n"
            "## 🖥️ 医学影像与诊断\n"
            "AI辅助诊断、影像分析的新突破 [citation:来源](URL)\n"
            "含：性能指标与临床验证情况\n\n"
            "## 💊 AI药物研发\n"
            "靶点发现、分子生成、临床试验优化等AI制药进展 [citation:来源](URL)\n\n"
            "## 🧪 数字病理与组学\n"
            "AI在基因组学、蛋白质组学、数字病理中的应用 [citation:来源](URL)\n\n"
            "## 📜 监管与伦理\n"
            "AI医疗器械审批动态或医学AI伦理讨论 [citation:来源](URL)\n\n"
            "---\n"
            "## 📎 Sources\n"
            "列出所有引用来源的完整链接\n\n"
            "关注已进入临床验证或有真实世界数据支撑的成果，"
            "过滤纯概念性工作，总字数400字以内。"
        ),
        "schedule": {"type": "daily", "hour": 18, "minute": 30},
        "notify_channels": ["telegram"],
    },
    {
        "id": "custom-query",
        "name": "✏️ 自定义任务",
        "description": "灵活配置自定义定时查询任务",
        "task_type": "custom_query",
        "task_prompt": "",
        "schedule": {"type": "daily", "hour": 9, "minute": 0},
        "notify_channels": ["email"],
    },
]

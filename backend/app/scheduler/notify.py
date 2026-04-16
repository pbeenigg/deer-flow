"""NotifyService — unified notification dispatch across multiple channels.

Supports email, WeChat Work webhook, Telegram Bot, and DingTalk webhook.
Follows the adapter pattern consistent with the Channel ABC in app.channels.
"""

from __future__ import annotations

import logging
import smtplib
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class NotifyChannel(ABC):
    """Base class for notification channel adapters."""

    @abstractmethod
    async def send(self, content: str, config: dict[str, Any], *, subject: str = "DeerFlow 定时推送") -> dict[str, Any]:
        """Send a notification. Returns a status dict."""


class EmailChannel(NotifyChannel):
    """Email notification via SMTP."""

    async def send(self, content: str, config: dict[str, Any], *, subject: str = "DeerFlow 定时推送") -> dict[str, Any]:
        smtp_host = config.get("smtp_host", "localhost")
        smtp_port = config.get("smtp_port", 587)
        smtp_user = config.get("smtp_user", "")
        smtp_password = config.get("smtp_password", "")
        from_email = config.get("from_email", smtp_user)
        to_email = config.get("to_email", "")
        use_tls = config.get("use_tls", True)

        if not to_email:
            return {"status": "failed", "error": "to_email is required"}

        msg = MIMEText(content, "html", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email

        try:
            if use_tls:
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP(smtp_host, smtp_port)

            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(from_email, [to_email], msg.as_string())
            server.quit()
            return {"status": "success"}
        except Exception as e:
            logger.exception("Email send failed")
            return {"status": "failed", "error": str(e)}


class WechatWebhookChannel(NotifyChannel):
    """WeChat Work (企业微信) webhook notification using markdown_v2."""

    @staticmethod
    def _escape_markdown_v2(text: str) -> str:
        escape_chars = r"\_*>~`|"
        result = []
        for ch in text:
            if ch in escape_chars:
                result.append(f"\\{ch}")
            else:
                result.append(ch)
        return "".join(result)

    @staticmethod
    def _format_markdown_v2(subject: str, content: str) -> str:
        escaped_subject = WechatWebhookChannel._escape_markdown_v2(subject)
        lines = content.split("\n")
        formatted_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("### "):
                heading = WechatWebhookChannel._escape_markdown_v2(stripped[4:])
                formatted_lines.append(f"### {heading}")
            elif stripped.startswith("## "):
                heading = WechatWebhookChannel._escape_markdown_v2(stripped[3:])
                formatted_lines.append(f"## {heading}")
            elif stripped.startswith("# "):
                heading = WechatWebhookChannel._escape_markdown_v2(stripped[2:])
                formatted_lines.append(f"# {heading}")
            elif stripped.startswith("- ") or stripped.startswith("* "):
                item_text = WechatWebhookChannel._escape_markdown_v2(stripped[2:])
                formatted_lines.append(f"- {item_text}")
            elif len(stripped) > 2 and stripped[0].isdigit() and stripped[1] == ".":
                item_text = WechatWebhookChannel._escape_markdown_v2(stripped[2:].strip())
                formatted_lines.append(f"{stripped[0]}. {item_text}")
            elif stripped == "---":
                formatted_lines.append("---")
            elif stripped == "":
                formatted_lines.append("")
            else:
                formatted_lines.append(WechatWebhookChannel._escape_markdown_v2(line))
        body = "\n".join(formatted_lines)
        return f"# {escaped_subject}\n\n{body}"

    async def send(self, content: str, config: dict[str, Any], *, subject: str = "DeerFlow 定时推送") -> dict[str, Any]:
        webhook_url = config.get("webhook_url", "")
        if not webhook_url:
            return {"status": "failed", "error": "webhook_url is required"}

        md_content = self._format_markdown_v2(subject, content)
        payload = {
            "msgtype": "markdown_v2",
            "markdown_v2": {"content": md_content},
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(webhook_url, json=payload)
                resp.raise_for_status()
                return {"status": "success", "response": resp.json()}
        except Exception as e:
            logger.exception("WeChat webhook send failed")
            return {"status": "failed", "error": str(e)}


class TelegramNotifyChannel(NotifyChannel):
    """Telegram Bot notification."""

    async def send(self, content: str, config: dict[str, Any], *, subject: str = "DeerFlow 定时推送") -> dict[str, Any]:
        bot_token = config.get("bot_token", "")
        chat_id = config.get("chat_id", "")
        if not bot_token or not chat_id:
            return {"status": "failed", "error": "bot_token and chat_id are required"}

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        text = f"**{subject}**\n\n{content}"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                return {"status": "success", "response": resp.json()}
        except Exception as e:
            logger.exception("Telegram notify send failed")
            return {"status": "failed", "error": str(e)}


class DingTalkChannel(NotifyChannel):
    """DingTalk (钉钉) webhook notification."""

    async def send(self, content: str, config: dict[str, Any], *, subject: str = "DeerFlow 定时推送") -> dict[str, Any]:
        webhook_url = config.get("webhook_url", "")
        if not webhook_url:
            return {"status": "failed", "error": "webhook_url is required"}

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": subject,
                "text": f"### {subject}\n\n{content}",
            },
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(webhook_url, json=payload)
                resp.raise_for_status()
                return {"status": "success", "response": resp.json()}
        except Exception as e:
            logger.exception("DingTalk send failed")
            return {"status": "failed", "error": str(e)}


class NotifyService:
    """Unified notification dispatch service.

    Routes notifications to the appropriate channel adapters
    based on the channel names specified in the task config.
    """

    _channels: dict[str, NotifyChannel] = {
        "email": EmailChannel(),
        "wechat": WechatWebhookChannel(),
        "telegram": TelegramNotifyChannel(),
        "dingtalk": DingTalkChannel(),
    }

    async def send(
        self,
        channels: list[str],
        content: str,
        config: dict[str, Any],
        *,
        task_name: str = "DeerFlow 定时推送",
    ) -> dict[str, dict[str, Any]]:
        """Send a notification to multiple channels.

        Returns a dict mapping channel name to its send result.
        """
        results: dict[str, dict[str, Any]] = {}

        for channel_name in channels:
            channel = self._channels.get(channel_name)
            if channel is None:
                results[channel_name] = {"status": "skipped", "error": f"Unknown channel: {channel_name}"}
                continue

            channel_config = config.get(channel_name, {})
            try:
                result = await channel.send(content, channel_config, subject=task_name)
                results[channel_name] = result
            except Exception as e:
                logger.exception("Notify channel %s failed", channel_name)
                results[channel_name] = {"status": "failed", "error": str(e)}

        return results

    @classmethod
    def get_available_channels(cls) -> list[str]:
        """Return the list of available notification channel names."""
        return list(cls._channels.keys())

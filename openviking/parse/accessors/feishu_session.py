# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Request-scoped Feishu configuration and API clients."""

import os
import ssl
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Generator, Optional, TypeVar
from urllib.parse import urlparse

from lark_oapi.core.cache import ICache
from requests.exceptions import ConnectTimeout, ReadTimeout, RequestException
from requests.exceptions import SSLError as RequestsSSLError

from openviking_cli.utils.config.parser_config import FeishuConfig
from openviking_cli.utils.logger import get_logger

from .feishu_token import (
    resolve_feishu_tenant_token_cache,
)

logger = get_logger(__name__)
_ResponseT = TypeVar("_ResponseT")
_GET_RETRY_DELAYS = (0.5, 1.0)


def _retryable_get_failure(request: Any, error: RequestException) -> bool:
    """只重试已确认来自本次只读 GET 的超时或 TLS 意外 EOF。"""
    method = getattr(getattr(request, "http_method", None), "name", "")
    actual_method = getattr(getattr(error, "request", None), "method", "")
    if str(method).upper() != "GET" or str(actual_method).upper() != "GET":
        # SDK 获取 token 的 POST 也可能失败；不能因外层是 GET 而重试写请求。
        return False
    if isinstance(error, (ConnectTimeout, ReadTimeout)):
        return True
    if not isinstance(error, RequestsSSLError):
        return False

    # requests/urllib3 会逐层包装 SSL 异常，不按错误文案猜测证书或 EOF。
    pending, seen = [error], set()
    unexpected_eof = False
    while pending:
        cause = pending.pop()
        if id(cause) in seen:
            continue
        seen.add(id(cause))
        if isinstance(cause, ssl.SSLCertVerificationError):
            return False
        if isinstance(cause, ssl.SSLEOFError) or (
            isinstance(cause, ssl.SSLError)
            and getattr(cause, "reason", None) == "UNEXPECTED_EOF_WHILE_READING"
        ):
            unexpected_eof = True
        nested = [getattr(cause, "__cause__", None), getattr(cause, "reason", None)]
        nested.extend(getattr(cause, "args", ()))
        pending.extend(value for value in nested if isinstance(value, BaseException))
    return unexpected_eof


def normalize_lark_file(options: dict[str, Any]) -> Optional[dict[str, str]]:
    """Select the same explicit identity for preflight and Understanding."""
    raw = options.get("lark_file")
    if raw is None:
        raw = {"user_access_token": options.get("feishu_access_token")}
    if not isinstance(raw, dict):
        raise ValueError("lark_file must be an object")
    auth = {
        key: value.strip()
        for key in ("user_access_token", "tenant_access_token")
        if isinstance(value := raw.get(key), str) and value.strip()
    }
    if len(auth) > 1:
        raise ValueError(
            "lark_file must contain exactly one of user_access_token or tenant_access_token"
        )
    return auth or None


def _default_api_domain(source_url: str) -> str:
    host = (urlparse(source_url).hostname or "").lower()
    if host.endswith(("larksuite.com", "larkoffice.com")):
        return "https://open.larksuite.com"
    return "https://open.feishu.cn"


@dataclass(frozen=True, slots=True)
class FeishuAccessContext:
    """Immutable configuration snapshot for one accessor operation."""

    config: FeishuConfig = field(repr=False)

    @classmethod
    def from_request(
        cls,
        source_url: str,
        *,
        config: Optional[FeishuConfig] = None,
    ) -> "FeishuAccessContext":
        effective_config = config
        if effective_config is None:
            effective_config = replace(
                FeishuConfig(),
                domain=_default_api_domain(source_url),
            )
        return cls(config=effective_config)


class FeishuApiSession:
    """Own lark-oapi clients for exactly one accessor operation."""

    def __init__(
        self,
        context: FeishuAccessContext,
        *,
        tenant_token_cache: ICache | None = None,
        tenant_access_token: Optional[str] = None,
    ):
        self.context = context
        self.tenant_access_token = tenant_access_token
        self._clients: dict[bool, Any] = {}
        self._tenant_token_cache = resolve_feishu_tenant_token_cache(tenant_token_cache)

    @staticmethod
    def call_with_read_retry(request: Any, call: Callable[[], _ResponseT]) -> _ResponseT:
        """最多额外尝试两次；HTTP/API 失败原样返回，最终网络异常原样抛出。"""
        for attempt in range(len(_GET_RETRY_DELAYS) + 1):
            try:
                return call()
            except RequestException as exc:
                if attempt == len(_GET_RETRY_DELAYS) or not _retryable_get_failure(request, exc):
                    raise
                # 不记录异常正文、URL、参数、请求头或任何鉴权值。
                logger.warning(
                    "Feishu只读GET临时网络失败，将重试 (%s, %d/%d)",
                    type(exc).__name__,
                    attempt + 2,
                    len(_GET_RETRY_DELAYS) + 1,
                )
                time.sleep(_GET_RETRY_DELAYS[attempt])
        raise AssertionError("不可达的 Feishu GET 重试状态")

    def client(self, *, use_user_token: bool) -> Any:
        cached = self._clients.get(use_user_token)
        if cached is not None:
            return cached

        try:
            import lark_oapi as lark
        except ImportError:
            raise ImportError(
                "lark-oapi is required for Feishu document parsing. "
                "Install it with: pip install lark-oapi>=1.0.0"
            )

        builder = (
            lark.Client.builder()
            .domain(self.context.config.domain)
            .timeout(self.context.config.request_timeout)
        )
        if use_user_token or self.tenant_access_token:
            builder = builder.enable_set_token(True)
        else:
            config = self.context.config
            app_id = config.app_id or os.getenv("FEISHU_APP_ID", "")
            app_secret = config.app_secret or os.getenv("FEISHU_APP_SECRET", "")
            if not app_id or not app_secret:
                raise ValueError(
                    "Feishu credentials not configured. Set FEISHU_APP_ID and "
                    "FEISHU_APP_SECRET environment variables, or configure in ov.conf."
                )
            builder = builder.app_id(app_id).app_secret(app_secret).cache(self._tenant_token_cache)

        client = builder.build()
        self._clients[use_user_token] = client
        return client

    @contextmanager
    def tenant_token_cache_scope(self) -> Generator[None, None, None]:
        """Bind Account credentials while lark-oapi obtains a tenant token."""
        config = self.context.config
        app_id = config.app_id or os.getenv("FEISHU_APP_ID", "")
        app_secret = config.app_secret or os.getenv("FEISHU_APP_SECRET", "")
        if not app_id or not app_secret:
            raise ValueError(
                "Feishu credentials not configured. Set FEISHU_APP_ID and "
                "FEISHU_APP_SECRET environment variables, or configure in ov.conf."
            )
        with self._tenant_token_cache.sdk_scope(
            app_id=app_id,
            app_secret=app_secret,
            domain=config.domain,
        ):
            yield

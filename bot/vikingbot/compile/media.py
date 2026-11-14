"""Bound non-Map evidence images while preserving requests that already fit."""

from __future__ import annotations

import base64
import binascii
import json

from vikingbot.compile.hashing import content_hash


def bound_inline_images(messages: list[dict], max_bytes: int) -> list[dict]:
    """On overflow, deduplicate decoded bytes and keep fitting images in source order.

    Request builders call this before submitting a batch of tool results. Existing
    input dictionaries are never mutated; requests within the limit are returned
    unchanged, including duplicate images and their original serialization.
    """
    from vikingbot.agent.loop import _inline_media_bytes

    if _inline_media_bytes(messages) <= max_bytes:
        return messages
    used, seen, bounded = 0, set(), []
    for message in messages:
        content = message.get("content")
        if not isinstance(content, list):
            bounded.append(message)
            continue
        metadata = None
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                try:
                    value = json.loads(part.get("text", ""))
                except (TypeError, ValueError):
                    continue
                if isinstance(value, dict) and "uri" in value:
                    metadata = value
                    break
        parts, notices = [], []
        for part in content:
            size = _inline_media_bytes([{"content": [part]}])
            if not size:
                parts.append(part)
                continue
            image_url = part.get("image_url")
            url = image_url.get("url") if isinstance(image_url, dict) else image_url
            try:
                raw = base64.b64decode(url.split(";base64,", 1)[1])
                fingerprint = content_hash(raw)
                reason = (
                    "duplicate of an earlier inline image"
                    if fingerprint in seen
                    else "inline media budget exhausted"
                    if used + size > max_bytes
                    else ""
                )
            except (ValueError, IndexError, binascii.Error):
                fingerprint, reason = content_hash(url), "invalid inline image encoding"
            if not reason:
                used += size
                seen.add(fingerprint)
                parts.append(part)
                continue
            label = metadata["uri"] if metadata else "image"
            notice = (
                f"Image evidence {label} (sha256:{fingerprint}) was not inlined: {reason} "
                f"({max_bytes} byte media budget). Use the text facts already supplied "
                "for this image record; duplicate images can use the earlier inline copy."
            )
            parts.append({"type": "text", "text": notice})
            notices.append(notice)
        if not notices:
            bounded.append(message)
            continue
        if (
            message.get("role") == "tool"
            and metadata is not None
            and not _inline_media_bytes([{"content": parts}])
        ):
            content = json.dumps(
                {**metadata, "image_inline_omitted": True, "media_budget_note": "\n".join(notices)},
                ensure_ascii=False,
            )
        else:
            content = parts
        bounded.append({**message, "content": content})
    return bounded

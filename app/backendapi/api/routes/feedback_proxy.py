"""Proxy user-facing feedback-agent routes through the platform API (port 8000).

Browsers hit RunPod :8000; server-to-server calls still use FEEDBACK_AGENT_BASE_URL (:5055).
Set FEEDBACK_PUBLIC_BASE_URL to the public :8000 origin so generated links match this proxy.
"""

from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, Request, Response

router = APIRouter(include_in_schema=False)


def _feedback_upstream() -> str:
    return (os.getenv("FEEDBACK_AGENT_BASE_URL") or "http://127.0.0.1:5055").rstrip("/")


def _forward_headers(request: Request) -> dict[str, str]:
    skip = {"host", "content-length", "connection", "transfer-encoding"}
    return {k: v for k, v in request.headers.items() if k.lower() not in skip}


async def _proxy(request: Request, upstream_path: str) -> Response:
    base = _feedback_upstream()
    path = upstream_path.lstrip("/")
    url = f"{base}/{path}" if path else base
    if request.url.query:
        url = f"{url}?{request.url.query}"

    body = await request.body()
    timeout = float(os.getenv("FEEDBACK_PROXY_TIMEOUT_SECONDS", "120"))

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        upstream = await client.request(
            request.method,
            url,
            headers=_forward_headers(request),
            content=body if body else None,
        )

    skip_resp = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in skip_resp}
    return Response(content=upstream.content, status_code=upstream.status_code, headers=headers)


@router.api_route("/api/reviews", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def proxy_api_reviews_root(request: Request) -> Response:
    return await _proxy(request, "api/reviews")


@router.api_route("/api/reviews/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def proxy_api_reviews(request: Request, path: str) -> Response:
    return await _proxy(request, f"api/reviews/{path}")


@router.api_route("/api/pose-markers", methods=["POST", "OPTIONS"])
async def proxy_pose_markers(request: Request) -> Response:
    return await _proxy(request, "api/pose-markers")


@router.get("/review/{path:path}")
async def proxy_review_pages(request: Request, path: str) -> Response:
    return await _proxy(request, f"review/{path}")


@router.get("/jobs/{review_id}")
async def proxy_feedback_job_page(request: Request, review_id: str) -> Response:
    return await _proxy(request, f"jobs/{review_id}")


@router.get("/share/{path:path}")
async def proxy_share_pages(request: Request, path: str) -> Response:
    return await _proxy(request, f"share/{path}")

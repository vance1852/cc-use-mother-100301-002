"""样品监管链的 HTTP/JSON 边界；未命中的路径回退到基础服务路由。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from polar_station_foundation.api import route as foundation_route
from polar_station_foundation.errors import DomainError
from polar_station_foundation.service import DomainService

from .service import CustodyService
from .storage import CustodyDatabase


def _receipt_response(receipt) -> tuple[int, dict[str, Any]]:
    return (200 if receipt.replayed else 201), receipt.__dict__


def route(custody: CustodyService, foundation: DomainService, method: str, path: str,
          body: dict[str, Any] | None, headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把样品监管链请求分派到领域服务，其余路径交给基础服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    query = parse_qs(parsed.query)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "POST" and parsed.path == "/custody/plans":
            return _receipt_response(custody.register_plan(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/custody/devices":
            return _receipt_response(custody.register_device(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/custody/references":
            return _receipt_response(custody.register_reference(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/custody/events":
            receipt = custody.submit_event(actor_id=actor_id, **body)
            status = 200 if receipt.replayed else (201 if receipt.status == "confirmed" else 202)
            return status, receipt.__dict__
        if method == "POST" and parsed.path == "/custody/quarantine/decisions":
            return _receipt_response(custody.decide_quarantine(actor_id=actor_id, **body))
        if method == "GET" and parsed.path == "/custody/containers":
            return 200, custody.get_container(query.get("container_id", [""])[0])
        if method == "GET" and parsed.path == "/custody/provenance":
            return 200, custody.get_provenance(query.get("result_id", [""])[0])
        if method == "GET" and parsed.path == "/custody/reconciliation":
            return 200, custody.reconcile_plan(query.get("plan_id", [""])[0])
        if method == "GET" and parsed.path == "/custody/overview":
            return 200, custody.admin_overview(query.get("site_id", [""])[0])
        if method == "GET" and parsed.path == "/custody/quarantine":
            return 200, {"items": custody.list_quarantine(site_id=query.get("site_id", [None])[0],
                                                          status=query.get("status", [None])[0])}
        if method == "GET" and parsed.path == "/custody/references":
            return 200, {"items": custody.list_reference_versions(query.get("doc_id", [""])[0])}
        if parsed.path.startswith("/custody/"):
            return 404, {"error": "route_not_found", "message": "接口不存在"}
        return foundation_route(foundation, method, path, body, headers)
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为监管链路由调用。"""

    custody: CustodyService
    foundation: DomainService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.custody, self.foundation, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动样品监管链 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动极地科考站样品监管链服务")
    parser.add_argument("--database", default="custody.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = CustodyDatabase(args.database)
    Handler.foundation = DomainService(database)
    Handler.custody = CustodyService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

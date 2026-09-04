"""Подопытный HTTP-сервис: RED, JSON-логи и OpenTelemetry."""
import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from aiohttp import web
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagators.textmap import default_getter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

PORT = int(os.getenv("PORT", "8000"))
SERVICE = os.getenv("OTEL_SERVICE_NAME", "red-demo")
# Адрес фиксирован: нагрузка всегда идет в этот процесс, а не на чужой хост.
SELF_URL = f"http://127.0.0.1:{PORT}/api/work"
PROPAGATOR = TraceContextTextMapPropagator()


class JsonFormatter(logging.Formatter):
    def format(self, record):
        ctx = trace.get_current_span().get_span_context()
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "service": SERVICE,
            "trace_id": format(ctx.trace_id, "032x") if ctx.is_valid else None,
            "span_id": format(ctx.span_id, "016x") if ctx.is_valid else None,
        }
        data.update(getattr(record, "fields", {}))
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
log = logging.getLogger(SERVICE)

# Для лабы сохраняем каждый трейс. SDK создает trace_id и без экспортера.
provider = TracerProvider(
    resource=Resource.create({"service.name": SERVICE}), sampler=ALWAYS_ON
)
EXPORT_ENABLED = bool(os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or
                      os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
if EXPORT_ENABLED:
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("red-demo")

REQUESTS = Counter("http_requests_total", "Completed HTTP requests",
                   ["method", "route", "status"])
ERRORS = Counter("http_errors_total", "HTTP responses with status 5xx",
                 ["method", "route"])
DURATION = Histogram("http_request_duration_seconds", "Handler duration in seconds",
                     ["method", "route"],
                     buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10))


@web.middleware
async def observe(request, handler):
    # Scrape и healthcheck не должны создавать искусственный пользовательский RPS.
    if request.path in {"/metrics", "/healthz"}:
        return await handler(request)
    resource = request.match_info.route.resource
    route = resource.canonical if resource is not None else "unmatched"
    method = request.method if request.method in {
        "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"
    } else "OTHER"
    started = time.perf_counter()
    parent = PROPAGATOR.extract(request.headers, getter=default_getter)
    with tracer.start_as_current_span(
        f"{method} {route}", context=parent, kind=SpanKind.SERVER,
        attributes={"http.request.method": method, "http.route": route},
    ) as span:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = web.json_response({"message": exc.reason}, status=exc.status,
                                         headers={k: v for k, v in exc.headers.items()
                                                  if k.lower() != "content-type"})
        except Exception as exc:
            span.record_exception(exc)
            log.exception("Unhandled request error")
            response = web.json_response({"message": "Internal server error"}, status=500)
        elapsed = time.perf_counter() - started
        status = response.status
        REQUESTS.labels(method, route, str(status)).inc()
        DURATION.labels(method, route).observe(elapsed)
        # Инициализируем также нулевое значение, чтобы серия была видна до ошибки.
        ERRORS.labels(method, route).inc(int(status >= 500))
        span.set_attribute("http.response.status_code", status)
        if status >= 500:
            span.set_status(Status(StatusCode.ERROR, f"HTTP {status}"))
        response.headers["X-Trace-ID"] = format(span.get_span_context().trace_id, "032x")
        log.log(logging.ERROR if status >= 500 else logging.INFO, "HTTP request completed",
                extra={"fields": {"method": method, "route": route,
                                  "status": status, "duration_seconds": round(elapsed, 6)}})
        return response


async def index(request):
    return web.Response(text=Path(__file__).with_name("index.html").read_text(),
                        content_type="text/html")


async def error(request):
    return web.json_response({"message": "Ошибка создана специально"}, status=500)


async def delay(request):
    seconds = random.uniform(1, 3)
    with tracer.start_as_current_span("slow-dependency", attributes={"delay.seconds": seconds}):
        await asyncio.sleep(seconds)
    return web.json_response({"message": "Задержка завершена", "delay_seconds": round(seconds, 3)})


async def work(request):
    return web.json_response({"message": "OK"})


LOAD_LOCK = web.AppKey("load_lock", asyncio.Lock)
CLIENT = web.AppKey("client", aiohttp.ClientSession)


async def load(request):
    lock = request.app[LOAD_LOCK]
    if lock.locked():
        return web.json_response({"message": "Нагрузка уже запущена"}, status=429)
    async with lock:
        started = time.perf_counter()

        async def worker():
            successes = 0
            # 10 работников по 20 запросов = 200 запросов за несколько секунд.
            for _ in range(20):
                with tracer.start_as_current_span(
                    "GET /api/work", kind=SpanKind.CLIENT,
                    attributes={"http.request.method": "GET", "url.full": SELF_URL},
                ) as span:
                    headers = {}
                    PROPAGATOR.inject(headers)
                    try:
                        async with request.app[CLIENT].get(SELF_URL, headers=headers) as response:
                            await response.read()
                            span.set_attribute("http.response.status_code", response.status)
                            successes += int(response.status == 200)
                            if response.status >= 400:
                                span.set_status(Status(StatusCode.ERROR))
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        span.record_exception(exc)
                        span.set_status(Status(StatusCode.ERROR))
                        log.warning("Self-request failed", extra={"fields": {"error": str(exc)}})
                await asyncio.sleep(0.1)
            return successes

        succeeded = sum(await asyncio.gather(*(worker() for _ in range(10))))
        return web.json_response({"message": "Нагрузка завершена", "sent": 200,
                                  "succeeded": succeeded, "failed": 200 - succeeded,
                                  "duration_seconds": round(time.perf_counter() - started, 3)})


async def metrics(request):
    return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def health(request):
    return web.json_response({"status": "ok"})


async def lifecycle(app):
    app[LOAD_LOCK] = asyncio.Lock()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
        app[CLIENT] = client
        log.info("Service started", extra={"fields": {"port": PORT, "otlp_export": EXPORT_ENABLED}})
        yield
    await asyncio.to_thread(provider.shutdown)
    log.info("Service stopped")


def create_app():
    app = web.Application(middlewares=[observe])
    app.cleanup_ctx.append(lifecycle)
    app.add_routes([web.get("/", index), web.post("/api/error", error),
                    web.post("/api/delay", delay), web.post("/api/load", load),
                    web.get("/api/work", work), web.get("/metrics", metrics),
                    web.get("/healthz", health)])
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT, access_log=None, print=None)

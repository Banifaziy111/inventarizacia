from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional

import httpx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError
from playwright.async_api import Page, async_playwright

logger = logging.getLogger(__name__)


class ApiRequest(BaseModel):
    """Описание одиночного HTTP-запроса к API, который можно выполнить до рендеринга."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Произвольное имя запроса, чтобы различать ответы.")
    url: HttpUrl = Field(..., description="Полный URL, который необходимо вызвать.")
    method: str = Field(default="GET", description="HTTP-метод (GET/POST/...).")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Произвольные заголовки для запроса.",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Query-параметры ?key=value.",
    )
    json_body: Optional[dict[str, Any]] = Field(
        default=None,
        alias="json",
        description="JSON-тело запроса (для POST/PUT/PATCH).",
    )
    data: Optional[dict[str, Any]] = Field(
        default=None,
        description="Форма (application/x-www-form-urlencoded).",
    )

    def normalized_method(self) -> str:
        return self.method.upper()


class ScraperConfig(BaseModel):
    """Конфигурация для парсинга страницы и вспомогательных API-запросов."""

    url: HttpUrl = Field(..., description="Целевая страница, которую нужно загрузить.")
    wait_selector: Optional[str] = Field(
        default=None,
        description="CSS-селектор элемента, появление которого означает готовность контента.",
    )
    row_selector: Optional[str] = Field(
        default=None,
        description="CSS-селектор строк таблицы или списка для извлечения.",
    )
    cell_selector: str = Field(
        default="td, th",
        description="CSS-селектор ячеек внутри строки.",
    )
    api_log: bool = Field(
        default=False,
        description="Сохранять ли ответы XHR/Fetch запросов, сделанных браузером.",
    )
    api_log_dir: Path = Field(
        default=Path("artifacts/responses"),
        description="Каталог, куда писать тела ответов при включенном api_log.",
    )
    screenshot_path: Optional[Path] = Field(
        default=None,
        description="Путь для сохранения скриншота после загрузки страницы.",
    )
    timeout_ms: int = Field(
        default=30_000,
        description="Таймаут ожидания загрузки/селекторов в миллисекундах.",
    )
    page_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Дополнительные HTTP-заголовки для запроса страницы.",
    )
    api_requests: list[ApiRequest] = Field(
        default_factory=list,
        description="Набор API-запросов, которые нужно выполнить перед рендерингом.",
    )


@dataclass(slots=True)
class ResponseLog:
    url: str
    status: int
    body_path: Optional[Path]


async def _collect_rows(page: Page, config: ScraperConfig) -> List[List[str]]:
    if not config.row_selector:
        return []

    rows = page.locator(config.row_selector)
    row_count = await rows.count()

    logger.info("Найдено %s строк по селектору %s", row_count, config.row_selector)
    result: List[List[str]] = []

    for idx in range(row_count):
        row = rows.nth(idx)
        cells = await row.locator(config.cell_selector).all_inner_texts()
        cleaned = [cell.strip() for cell in cells if cell.strip()]
        if cleaned:
            result.append(cleaned)

    return result


def _prepare_response_logging(
    page: Page, config: ScraperConfig
) -> tuple[list[ResponseLog], list[asyncio.Task[Any]]]:
    api_logs: list[ResponseLog] = []
    tasks: list[asyncio.Task[Any]] = []
    config.api_log_dir.mkdir(parents=True, exist_ok=True)

    async def process_response(response):
        if response.request.resource_type not in {"xhr", "fetch"}:
            return
        body_path: Optional[Path] = None
        try:
            body = await response.body()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Не удалось получить тело ответа %s: %s", response.url, exc)
            body = b""
        if body:
            safe_name = (
                response.url.replace("://", "_").replace("/", "_").replace("?", "_")
            )
            body_path = config.api_log_dir / f"{safe_name[:150]}.bin"
            body_path.write_bytes(body)
        api_logs.append(ResponseLog(response.url, response.status, body_path))

    def handle_response(response):
        tasks.append(asyncio.create_task(process_response(response)))

    page.on("response", handle_response)
    return api_logs, tasks


async def _fetch_api_data(api_requests: Iterable[ApiRequest]) -> list[dict[str, Any]]:
    requests = list(api_requests)
    if not requests:
        return []

    results: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for req in requests:
            try:
                response = await client.request(
                    req.normalized_method(),
                    str(req.url),
                    params=req.params or None,
                    headers=req.headers or None,
                    json=req.json_body,
                    data=req.data,
                )
                try:
                    payload: Any = response.json()
                except ValueError:
                    payload = response.text
                results.append(
                    {
                        "name": req.name,
                        "url": str(req.url),
                        "status": response.status_code,
                        "data": payload,
                        "headers": dict(response.headers),
                    }
                )
            except httpx.HTTPError as exc:
                logger.error("Ошибка при запросе %s: %s", req.name, exc)
                results.append(
                    {
                        "name": req.name,
                        "url": str(req.url),
                        "status": 0,
                        "data": None,
                        "error": str(exc),
                    }
                )
    return results


async def scrape(config: ScraperConfig) -> dict[str, Any]:
    logger.info("Начало парсинга...")
    api_results = await _fetch_api_data(config.api_requests)

    rows: List[List[str]] = []
    response_logs: list[ResponseLog] = []
    response_tasks: list[asyncio.Task[Any]] = []
    error: Optional[str] = None

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                extra_http_headers=config.page_headers or None
            )
            page = await context.new_page()

            if config.api_log:
                response_logs, response_tasks = _prepare_response_logging(page, config)

            logger.info("Переход на %s", config.url)
            await page.goto(
                str(config.url), wait_until="networkidle", timeout=config.timeout_ms
            )

            if config.wait_selector:
                logger.info("Ожидание селектора %s", config.wait_selector)
                await page.wait_for_selector(
                    config.wait_selector, timeout=config.timeout_ms
                )

            if config.screenshot_path:
                config.screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(config.screenshot_path), full_page=True)
                logger.info("Скриншот сохранён: %s", config.screenshot_path)

            rows = await _collect_rows(page, config)

            if response_tasks:
                await asyncio.gather(*response_tasks, return_exceptions=True)

            await context.close()
            await browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка при парсинге страницы: %s", exc)
        error = str(exc)

    logger.info("Парсинг завершён. Строк: %d, API результатов: %d", len(rows), len(api_results))

    return {
        "rows": rows,
        "row_count": len(rows),
        "api_results": api_results,
        "response_logs": [
            {
                "url": log.url,
                "status": log.status,
                "body_path": str(log.body_path) if log.body_path else None,
            }
            for log in response_logs
        ],
        "error": error,
    }


def _parse_headers(entries: Optional[Iterable[str]]) -> dict[str, str]:
    headers: dict[str, str] = {}
    if not entries:
        return headers

    for raw in entries:
        if ":" not in raw:
            raise SystemExit(
                f"Некорректный формат заголовка '{raw}'. Используйте 'Имя: Значение'."
            )
        key, value = raw.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Headless-парсер для SPA страницы с помощью Playwright."
    )
    parser.add_argument("--config", type=Path, help="Путь до JSON/YAML конфигурации.")
    parser.add_argument("--url", help="URL страницы (если конфиг не задан).")
    parser.add_argument("--wait-selector", dest="wait_selector", help="CSS-селектор ожидания.")
    parser.add_argument(
        "--row-selector",
        dest="row_selector",
        help="CSS-селектор строк таблицы/списка.",
    )
    parser.add_argument(
        "--cell-selector",
        dest="cell_selector",
        default=None,
        help="CSS-селектор ячеек внутри строки.",
    )
    parser.add_argument(
        "--api-log",
        dest="api_log",
        action="store_true",
        help="Включить сохранение XHR/Fetch ответов.",
    )
    parser.add_argument(
        "--screenshot",
        dest="screenshot_path",
        type=Path,
        help="Сохранить скриншот страницы по указанному пути.",
    )
    parser.add_argument(
        "--timeout",
        dest="timeout_ms",
        type=int,
        default=None,
        help="Таймаут ожиданий (мс).",
    )
    parser.add_argument(
        "--header",
        dest="headers",
        action="append",
        default=None,
        help="Дополнительный заголовок для страницы (формат 'Имя: Значение'). "
        "Можно указать несколько раз.",
    )
    parser.add_argument(
        "--api-url",
        dest="api_urls",
        action="append",
        default=None,
        help="Дополнительный GET-запрос к API. Можно указать несколько раз.",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        type=Path,
        default=None,
        help="Путь для сохранения результата в JSON.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробный лог (уровень DEBUG).",
    )
    return parser.parse_args()


def _load_config_from_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Конфигурация {path} не найдена.")
    text = path.read_text(encoding="utf-8")
    if path.suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:  # noqa: WPS420
            raise RuntimeError("Для YAML-конфигов установите пакет pyyaml.") from exc
        return yaml.safe_load(text)
    return json.loads(text)


def _build_config(args: argparse.Namespace) -> ScraperConfig:
    raw: dict[str, Any] = {}
    if args.config:
        raw.update(_load_config_from_file(args.config))

    overrides = {
        key: value
        for key, value in {
            "url": args.url,
            "wait_selector": args.wait_selector,
            "row_selector": args.row_selector,
            "cell_selector": args.cell_selector,
            "api_log": args.api_log or None,
            "screenshot_path": args.screenshot_path,
            "timeout_ms": args.timeout_ms,
        }.items()
        if value is not None
    }
    raw.update(overrides)

    headers_override = _parse_headers(args.headers)
    if headers_override:
        merged_headers = dict(raw.get("page_headers", {}))
        merged_headers.update(headers_override)
        raw["page_headers"] = merged_headers

    if args.api_urls:
        existing = list(raw.get("api_requests", []))
        start_idx = len(existing)
        for offset, url in enumerate(args.api_urls, start=1):
            existing.append(
                {
                    "name": f"cli_api_{start_idx + offset}",
                    "url": url,
                    "method": "GET",
                }
            )
        raw["api_requests"] = existing

    try:
        return ScraperConfig(**raw)
    except ValidationError as exc:  # noqa: WPS420
        raise SystemExit(f"Ошибка конфигурации: {exc}") from exc


async def main_async() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    config = _build_config(args)
    result = await scrape(config)

    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("Результат сохранён в %s", args.output_path)
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()


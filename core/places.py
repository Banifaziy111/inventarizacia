"""
core/places.py
Логика получения данных о складском месте (МХ) по числовому mx_id или строковому mx_code.
Используется маршрутом GET /api/place/<place_cod>.
"""

import logging
import re

from flask import jsonify
import psycopg2

from core.db import ensure_warehouse_places_mx_status

logger = logging.getLogger(__name__)

_SELECT_FIELDS = """
    SELECT
        wh_id,
        mx_id        AS place_cod,
        mx_code      AS place_name,
        0            AS qty_shk,
        storage_type,
        box_type,
        dimensions,
        category,
        floor,
        row_num,
        section,
        shelf,
        cell,
        current_volume,
        current_occupancy,
        mx_status,
        updated_at
    FROM warehouse_places
"""


def resolve_mx_type(storage_type, box_type, dimensions, category=None):
    """Определяем тип МХ — только «Полка» или «Короб»; при неизвестном возвращаем None."""
    for val in (storage_type, box_type, category):
        if not val:
            continue
        s = str(val).lower()
        if "короб" in s or "box" in s:
            return "Короб"
        if "полка" in s or "shelf" in s or "стеллаж" in s:
            return "Полка"
    if dimensions:
        try:
            parts = str(dimensions).replace("х", "x").replace("Х", "x").split("x")
            nums = [int(p.strip()) for p in parts if p.strip().isdigit()]
            if nums:
                return "Полка" if max(nums) > 900 else "Короб"
        except (ValueError, TypeError):
            pass
    if storage_type or box_type or category:
        return "Короб"
    return None


def get_place_handler(place_cod, get_db_fn):
    """
    Основная логика GET /api/place/<place_cod>.

    :param place_cod: строка из URL (числовой ID или mx_code).
    :param get_db_fn: callable, возвращающий активное соединение psycopg2.
    :returns: Flask Response.
    """
    # Разбираем тип идентификатора
    try:
        place_cod_int = int(place_cod)
        search_by_id = True
        place_cod_str = None
    except ValueError:
        search_by_id = False
        place_cod_int = None
        place_cod_str = place_cod.strip().upper()
        if not re.match(r"^[\u0410-\u042F\u0401A-Z0-9.\-]+$", place_cod_str):
            return jsonify({"error": "Некорректный формат кода МХ"}), 400

    try:
        conn = get_db_fn()
        ensure_warehouse_places_mx_status(conn)
        with conn.cursor() as cur:
            row = _fetch_place_row(cur, search_by_id, place_cod_int, place_cod_str)

            if not row:
                return jsonify({"error": "Место не найдено"}), 404

            mx_type = resolve_mx_type(
                row.get("storage_type"),
                row.get("box_type"),
                row.get("dimensions"),
                row.get("category"),
            ) or "—"

            admin_status = _fetch_admin_status(cur, row)

            return jsonify(
                {
                    "place_cod": row["place_cod"],
                    "place_name": row["place_name"],
                    "qty_shk": row["qty_shk"],
                    "mx_type": mx_type,
                    "storage_type": row["storage_type"],
                    "box_type": row["box_type"],
                    "dimensions": row["dimensions"],
                    "category": row["category"] or "Не указана",
                    "floor": row["floor"],
                    "row_num": row["row_num"],
                    "section": row["section"],
                    "shelf": row["shelf"],
                    "cell": row["cell"],
                    "current_volume": float(row["current_volume"]) if row["current_volume"] else None,
                    "current_occupancy": row["current_occupancy"],
                    "mx_status": row.get("mx_status"),
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "admin_status": admin_status,
                }
            )

    except psycopg2.Error:
        logger.exception("База данных недоступна при получении МХ")
        return jsonify({"error": "Справочник временно недоступен (нет связи с БД). Используйте локальный кэш или повторите позже."}), 503
    except Exception:
        logger.exception("Ошибка при получении данных о месте")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500


# ──────────────────────── внутренние вспомогательные функции ────────────────────────


def _fetch_place_row(cur, search_by_id: bool, place_cod_int, place_cod_str):
    """Ищем место в warehouse_places; возвращаем первую найденную строку или None."""
    if search_by_id:
        cur.execute(_SELECT_FIELDS + "WHERE mx_id = %s", (place_cod_int,))
        return cur.fetchone()

    # Точное совпадение с TRIM
    cur.execute(
        _SELECT_FIELDS + "WHERE UPPER(TRIM(mx_code)) = UPPER(TRIM(%s))",
        (place_cod_str,),
    )
    row = cur.fetchone()
    if row:
        return row

    # Запасной вариант: без пробелов внутри
    cur.execute(
        _SELECT_FIELDS + "WHERE REPLACE(UPPER(mx_code), ' ', '') = REPLACE(UPPER(TRIM(%s)), ' ', '') LIMIT 1",
        (place_cod_str,),
    )
    return cur.fetchone()


def _fetch_admin_status(cur, row):
    """Возвращает статус исправления МХ из repaired_places или None."""
    wh_id = row.get("wh_id")
    if wh_id is None:
        return None
    cur.execute(
        "SELECT status FROM repaired_places WHERE wh_id = %s AND place_cod = %s",
        (wh_id, row["place_cod"]),
    )
    status_row = cur.fetchone()
    return status_row.get("status") if status_row else None

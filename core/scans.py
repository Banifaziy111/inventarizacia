"""
core/scans.py
Логика фиксации результата сканирования (POST /api/scan/complete).
"""

import base64
import logging

import psycopg2
from flask import jsonify, request

from core.db import ensure_shift_start_table, safe_rollback

logger = logging.getLogger(__name__)


def _decode_photos(photos_list, single_photo, place_cod):
    """
    Декодирует список base64-фото (или одиночное фото для обратной совместимости).
    Возвращает [(bytes, filename), ...].
    """
    decoded = []

    def _decode_one(photo_str: str, index: int):
        if not photo_str:
            return
        try:
            if "," in photo_str:
                header, encoded = photo_str.split(",", 1)
            else:
                header, encoded = "", photo_str
            raw = base64.b64decode(encoded)
            ext = "png" if "png" in header else "jpg"
            decoded.append((raw, f"{place_cod}_{index + 1}.{ext}"))
        except Exception as exc:
            logger.error("Ошибка декодирования фото #%s: %s", index + 1, exc)

    if isinstance(photos_list, list) and photos_list:
        for idx, p in enumerate(photos_list):
            _decode_one(p, idx)
    elif single_photo:
        _decode_one(single_photo, 0)

    return decoded


def complete_scan_handler(get_db_fn):
    """
    Основная логика POST /api/scan/complete.

    :param get_db_fn: callable, возвращающий активное соединение psycopg2.
    :returns: Flask Response.
    """
    data = request.get_json() or {}
    badge = data.get("badge")
    place_cod = data.get("place_cod")
    fact_qty = data.get("fact_qty")
    status = data.get("status")
    comment = data.get("comment", "")
    discrepancy_reason = data.get("discrepancy_reason", "")
    photos = data.get("photos") or []
    photo_raw = data.get("photo")
    force_duplicate = bool(data.get("force_duplicate"))

    status_norm = str(status).strip().lower() if status is not None else ""

    if not badge or not place_cod or not status_norm:
        return jsonify({"error": "Недостаточно данных"}), 400

    try:
        place_cod_int = int(place_cod)
    except (TypeError, ValueError):
        return jsonify({"error": "Некорректный place_cod"}), 400

    decoded_photos = _decode_photos(photos, photo_raw, place_cod)
    photo_data = decoded_photos[0][0] if decoded_photos else None
    photo_filename = decoded_photos[0][1] if decoded_photos else None

    conn = None
    try:
        conn = get_db_fn()
        with conn.cursor() as cur:
            # Получаем название места из справочника
            cur.execute(
                "SELECT mx_code AS place_name, 0 AS qty_shk FROM warehouse_places WHERE mx_id = %s",
                (place_cod_int,),
            )
            place_row = cur.fetchone()

            # Канонизируем статус, чтобы "OK"/" ok "/etc. не ломали логику расхождений.
            status = status_norm
            has_discrepancy = status_norm != "ok"
            qty_shk_db = place_row["qty_shk"] if place_row else None
            qty_fact_int = None
            if fact_qty is not None:
                try:
                    qty_fact_int = int(fact_qty)
                except (TypeError, ValueError):
                    pass

            if qty_shk_db is not None and qty_fact_int is not None:
                try:
                    has_discrepancy = has_discrepancy or int(qty_shk_db) != qty_fact_int
                except ValueError:
                    pass

            # Проверка дубликата в рамках текущей смены
            ensure_shift_start_table(conn)
            cur.execute(
                """
                INSERT INTO shift_start (badge, started_at)
                SELECT %s, COALESCE(
                    (SELECT MIN(created_at) FROM inventory_results WHERE badge = %s),
                    NOW()
                )
                ON CONFLICT (badge) DO NOTHING
                """,
                (badge, badge),
            )
            cur.execute(
                """
                SELECT 1 FROM inventory_results ir
                WHERE ir.badge = %s AND ir.place_cod = %s
                  AND ir.created_at >= (SELECT started_at FROM shift_start WHERE shift_start.badge = %s)
                LIMIT 1
                """,
                (badge, place_cod_int, badge),
            )
            if cur.fetchone():
                if force_duplicate:
                    # Пользователь подтвердил, что это сознательная задвойка.
                    marker = "[Задвойка подтверждена]"
                    comment_text = (comment or "").strip()
                    if marker not in comment_text:
                        comment = f"{comment_text} {marker}".strip() if comment_text else marker
                else:
                    return jsonify(
                        {
                            "error": "Этот МХ уже отсканирован в текущей смене. Это задвойка?",
                            "code": "duplicate_in_shift",
                            "confirm_required": True,
                        }
                    ), 409

            # Сохраняем результат
            cur.execute(
                """
                INSERT INTO inventory_results
                (badge, place_cod, place_name, qty_shk_db, qty_shk_fact, status, has_discrepancy,
                 photo_data, photo_filename, discrepancy_reason, comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING result_id, created_at
                """,
                (
                    badge,
                    place_cod_int,
                    place_row["place_name"] if place_row else None,
                    qty_shk_db,
                    qty_fact_int,
                    status,
                    has_discrepancy,
                    psycopg2.Binary(photo_data) if photo_data else None,
                    photo_filename,
                    discrepancy_reason or None,
                    comment or None,
                ),
            )
            inserted = cur.fetchone()

            # Сохраняем все фото отдельной таблицей
            for raw, fname in decoded_photos:
                cur.execute(
                    """
                    INSERT INTO inventory_result_photos
                    (result_id, badge, place_cod, photo_data, photo_filename)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (inserted["result_id"], badge, place_cod_int, psycopg2.Binary(raw), fname),
                )

            conn.commit()

        logger.info("Скан сохранен: badge=%s place=%s status=%s", badge, place_cod, status)
        return jsonify(
            {
                "success": True,
                "result": {
                    "id": inserted["result_id"],
                    "place_cod": place_cod_int,
                    "place_name": place_row["place_name"] if place_row else None,
                    "qty_db": qty_shk_db,
                    "qty_fact": qty_fact_int,
                    "status": status,
                    "has_discrepancy": has_discrepancy,
                    "has_photo": photo_data is not None,
                    "is_duplicate": force_duplicate,
                    "comment": comment,
                    "created_at": inserted["created_at"].isoformat()
                    if inserted and inserted["created_at"]
                    else None,
                },
            }
        )

    except psycopg2.Error as e:
        safe_rollback(conn)
        logger.exception("Ошибка БД при сохранении скана")
        return jsonify({"error": f"Ошибка базы данных: {e}"}), 500
    except Exception as e:
        safe_rollback(conn)
        logger.exception("Ошибка при сохранении скана")
        return jsonify({"error": str(e)}), 500

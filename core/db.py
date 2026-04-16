import logging

logger = logging.getLogger(__name__)


def safe_rollback(conn):
    """Безопасный rollback транзакции."""
    if conn and not conn.closed:
        try:
            conn.rollback()
        except Exception as e:
            logger.error("Ошибка при rollback: %s", e)


def ensure_shift_start_table(conn):
    """Создаёт таблицу shift_start при первом обращении (граница смены для блокировки дубликатов МХ)."""
    if not conn or conn.closed:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shift_start (
                    badge TEXT NOT NULL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT (NOW())
                )
            """)
    except Exception as e:
        safe_rollback(conn)
        logger.warning("Не удалось создать shift_start: %s", e)


def ensure_warehouse_places_mx_status(conn):
    """Добавляет колонку mx_status в warehouse_places при первом обращении (Статус МХ: Активно, есть/нет товаров)."""
    if not conn or conn.closed:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE warehouse_places
                ADD COLUMN IF NOT EXISTS mx_status VARCHAR(150)
            """)
    except Exception as e:
        safe_rollback(conn)
        logger.warning("Не удалось добавить mx_status: %s", e)


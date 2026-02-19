"""Модуль для работы с PostgreSQL."""

import logging
import re
from typing import Any, Optional

import asyncpg

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Конфигурация подключения к БД."""
    
    def __init__(
        self,
        host: str = "31.207.77.167",
        port: int = 5432,
        database: str = "botdb",
        user: str = "aperepechkin",
        password: str = "password",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    @property
    def dsn(self) -> str:
        """Строка подключения."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


def extract_plt_id(data_mx: str) -> Optional[str]:
    """
    Извлекает plt_id из строки data_mx.
    
    Формат: "3****8002,employee_id,employee_name,date"
    Извлекаем: "3****8002" (до первой запятой)
    
    Args:
        data_mx: Строка с данными
        
    Returns:
        plt_id или None если не найден
    """
    if not data_mx or data_mx.strip() == "":
        return None
    
    # Берём всё до первой запятой
    parts = data_mx.split(",")
    if parts:
        plt_id = parts[0].strip()
        # Проверяем, что это похоже на ID (начинается с цифры)
        if plt_id and re.match(r'^\d', plt_id):
            return plt_id
    
    return None


async def create_table(pool: asyncpg.Pool) -> None:
    """Создает таблицы warehouse_places и active_tasks если их нет.

    Также гарантирует наличие колонок под новый формат данных:
    place_name, pallet_id, qty_shk_all, qty_plt.
    """
    async with pool.acquire() as conn:
        # Проверяем существование таблицы warehouse_places
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'warehouse_places'
            );
            """
        )

        if exists:
            logger.info("Таблица warehouse_places уже существует")
        else:
            logger.info("Создание таблицы warehouse_places...")
            await conn.execute(
                """
                CREATE TABLE warehouse_places (
                    place_cod BIGINT PRIMARY KEY,
                    place_name VARCHAR(255) NOT NULL,
                    qty_shk INTEGER NOT NULL DEFAULT 0,
                    plt_id VARCHAR(50),
                    -- новые поля под формат api_result.json
                    pallet_id VARCHAR(50),
                    qty_shk_all INTEGER NOT NULL DEFAULT 0,
                    qty_plt INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            logger.info("✅ Таблица warehouse_places создана")

        # Гарантируем наличие новых колонок, даже если таблица была создана раньше
        await conn.execute(
            """
            ALTER TABLE warehouse_places
            ADD COLUMN IF NOT EXISTS pallet_id VARCHAR(50)
            """
        )
        await conn.execute(
            """
            ALTER TABLE warehouse_places
            ADD COLUMN IF NOT EXISTS qty_shk_all INTEGER NOT NULL DEFAULT 0
            """
        )
        await conn.execute(
            """
            ALTER TABLE warehouse_places
            ADD COLUMN IF NOT EXISTS qty_plt INTEGER NOT NULL DEFAULT 0
            """
        )

        # Создаём таблицу активных заданий
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_tasks (
                task_id SERIAL PRIMARY KEY,
                zone_prefix VARCHAR(50) NOT NULL,
                badge VARCHAR(100) NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                status VARCHAR(20) DEFAULT 'active'
            )
            """
        )
        logger.info("✅ Таблица active_tasks готова")

        # Создаём индексы для быстрого поиска
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_warehouse_places_plt_id
            ON warehouse_places(plt_id)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_warehouse_places_pallet_id
            ON warehouse_places(pallet_id)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_warehouse_places_name
            ON warehouse_places(place_name)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_active_tasks_zone
            ON active_tasks(zone_prefix, status)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_active_tasks_expires
            ON active_tasks(expires_at)
            """
        )

        logger.info("Таблицы и индексы готовы")


async def upsert_places(
    pool: asyncpg.Pool,
    places: list[dict[str, Any]]
) -> dict[str, int]:
    """
    Обновляет таблицу складских мест по данным из JSON.

    Новый режим работы:
    - при каждом запуске таблица полностью очищается;
    - затем загружаются только актуальные данные из JSON.

    Формат ожидаемых данных (один элемент списка places):
        {
            "place_name": str,
            "pallet_id": str,
            "qty_shk_all": int,
            "qty_plt": int,
            ... другие поля игнорируются ...
        }

    При загрузке данные мапятся в таблицу warehouse_places так:
        place_cod  – генерируется последовательно (1, 2, 3, ...)
        place_name – из JSON.place_name
        qty_shk    – из JSON.qty_shk_all
        plt_id     – из JSON.pallet_id
        pallet_id  – из JSON.pallet_id
        qty_shk_all – из JSON.qty_shk_all
        qty_plt    – из JSON.qty_plt
    
    Args:
        pool: Пул подключений к БД
        places: Список мест со склада
        
    Returns:
        Статистика: {"inserted": N, "updated": M, "skipped": K}
    """
    stats = {"inserted": 0, "updated": 0, "skipped": 0}

    # Подготовка данных
    rows: list[tuple[int, str, Optional[str], int, int]] = []
    for idx, place in enumerate(places, start=1):
        # Поддерживаем как новый формат (qty_shk_all, pallet_id, qty_plt),
        # так и старый формат (qty_shk, data_mx).
        place_name = place.get("place_name")
        pallet_id = place.get("pallet_id")

        # qty_shk_all: сначала берём из нового поля, если его нет — из qty_shk
        qty_shk_all = place.get("qty_shk_all")
        if qty_shk_all is None:
            qty_shk_all = place.get("qty_shk", 0)
        qty_shk_all = qty_shk_all or 0

        # qty_plt: в старом формате его нет — считаем 1, если есть товар, иначе 0
        qty_plt = place.get("qty_plt")
        if qty_plt is None:
            qty_plt = 1 if qty_shk_all > 0 else 0
        qty_plt = qty_plt or 0

        # Если pallet_id пустой, пытаемся вытащить его из data_mx
        if not pallet_id:
            data_mx = place.get("data_mx")
            if data_mx:
                pallet_id = extract_plt_id(data_mx)

        # place_name обязателен, остальное можем оставить пустым
        if not place_name:
            stats["skipped"] += 1
            continue

        # Если в JSON есть реальный place_cod — используем его, иначе падаем обратно на счётчик idx
        place_cod_raw = place.get("place_cod")
        place_cod: Optional[int]
        if place_cod_raw is None:
            place_cod = idx
        else:
            try:
                place_cod = int(place_cod_raw)
            except (TypeError, ValueError):
                place_cod = idx

        rows.append((place_cod, place_name, pallet_id, qty_shk_all, qty_plt))

    if not rows:
        logger.warning("Нет данных для импорта")
        return stats

    logger.info(
        "Начало перезагрузки таблицы warehouse_places, записей: %d...",
        len(rows),
    )

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. Полностью очищаем таблицу
            await conn.execute("TRUNCATE TABLE warehouse_places;")

            # 2. Временная таблица под новый формат данных
            await conn.execute(
                """
                CREATE TEMP TABLE temp_warehouse_places (
                    place_cod BIGINT,
                    place_name VARCHAR(255),
                    pallet_id VARCHAR(50),
                    qty_shk_all INTEGER,
                    qty_plt INTEGER
                ) ON COMMIT DROP
                """
            )

            # 3. Массовая вставка во временную таблицу
            await conn.copy_records_to_table(
                "temp_warehouse_places",
                records=rows,
                columns=[
                    "place_cod",
                    "place_name",
                    "pallet_id",
                    "qty_shk_all",
                    "qty_plt",
                ],
            )

            # 4. Перенос во боевую таблицу с маппингом полей
            await conn.execute(
                """
                INSERT INTO warehouse_places (
                    place_cod,
                    place_name,
                    qty_shk,
                    plt_id,
                    pallet_id,
                    qty_shk_all,
                    qty_plt,
                    updated_at
                )
                SELECT
                    place_cod,
                    place_name,
                    -- qty_shk для бота = qty_shk_all из API
                    qty_shk_all AS qty_shk,
                    -- старое поле plt_id для бота = pallet_id из API
                    pallet_id AS plt_id,
                    pallet_id,
                    qty_shk_all,
                    qty_plt,
                    CURRENT_TIMESTAMP
                FROM temp_warehouse_places
                """
            )

            stats["inserted"] = len(rows)

    logger.info(
        "Импорт завершён (режим перезаливки): вставлено=%d, обновлено=%d, пропущено=%d",
        stats["inserted"],
        stats["updated"],
        stats["skipped"],
    )

    return stats


async def get_connection_pool(config: DatabaseConfig) -> asyncpg.Pool:
    """Создаёт пул подключений к БД."""
    pool = await asyncpg.create_pool(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        min_size=2,
        max_size=10,
        command_timeout=300,  # Увеличен таймаут до 5 минут для больших операций
    )
    logger.info("Пул подключений к БД создан")
    return pool


async def get_task_for_user(pool: asyncpg.Pool, zone_size: int = 50) -> dict:
    """
    Получает задание для пользователя - группу соседних МХ.
    
    Args:
        pool: Пул подключений
        zone_size: Количество мест в задании
        
    Returns:
        Словарь с информацией о задании
    """
    async with pool.acquire() as conn:
        # Упрощенный быстрый запрос: берем случайное место и от него соседние
        rows = await conn.fetch("""
            WITH random_place AS (
                SELECT place_name
                FROM warehouse_places
                WHERE place_name IS NOT NULL AND place_name != ''
                ORDER BY RANDOM()
                LIMIT 1
            )
            SELECT 
                w.place_cod,
                w.place_name,
                w.qty_shk,
                w.plt_id
            FROM warehouse_places w, random_place r
            WHERE w.place_name LIKE SUBSTRING(r.place_name, 1, 9) || '%'
                AND w.place_name IS NOT NULL
            ORDER BY w.place_name
            LIMIT $1
        """, zone_size)
        
        if not rows or len(rows) == 0:
            # Фоллбэк: просто случайные места
            rows = await conn.fetch("""
                SELECT place_cod, place_name, qty_shk, plt_id
                FROM warehouse_places
                WHERE place_name IS NOT NULL
                ORDER BY RANDOM()
                LIMIT $1
            """, zone_size)
        
        places = [
            {
                'place_cod': row['place_cod'],
                'place_name': row['place_name'],
                'qty_shk': row['qty_shk'],
                'plt_id': row['plt_id']
            }
            for row in rows
        ]
        
        if places and places[0]['place_name']:
            # Определяем зону по первому месту
            zone_prefix = places[0]['place_name'][:9] if len(places[0]['place_name']) >= 9 else places[0]['place_name']
            zone_info = {
                'zone': zone_prefix,
                'total_places': len(places),
                'places': places
            }
        else:
            zone_info = {
                'zone': 'Разные зоны',
                'total_places': len(places),
                'places': places
            }
        
        return zone_info


async def test_connection(config: DatabaseConfig) -> bool:
    """Тестирует подключение к БД."""
    try:
        pool = await get_connection_pool(config)
        async with pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            logger.info("Подключение к БД успешно: %s", version)
        await pool.close()
        return True
    except Exception as exc:
        logger.error("Ошибка подключения к БД: %s", exc)
        return False


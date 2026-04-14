-- Оптимизация warehouse_places: чистка, индексы, mat view, контроль колонок
-- Запуск:
--   psql "$DATABASE_URL" -f optimize_warehouse_places.sql
--
-- Важно:
-- - Скрипт безопасный по умолчанию (не дропает колонки автоматически).
-- - Блок "DROP COLUMN" ниже выполняется только если вручную включить переменную.
-- - Материализованное представление создаётся и рефрешится отдельно.

BEGIN;

-- 0) Мини-чистка текстовых полей: пустые строки -> NULL (меньше мусора и лучше селективность).
UPDATE warehouse_places
SET
    warehouse_name = NULLIF(BTRIM(warehouse_name), ''),
    storage_type   = NULLIF(BTRIM(storage_type), ''),
    box_type       = NULLIF(BTRIM(box_type), ''),
    category       = NULLIF(BTRIM(category), ''),
    dimensions     = NULLIF(BTRIM(dimensions), ''),
    current_occupancy = NULLIF(BTRIM(current_occupancy), ''),
    mx_status      = NULLIF(BTRIM(mx_status), '')
WHERE
    warehouse_name IS NULL OR warehouse_name = '' OR
    storage_type IS NULL OR storage_type = '' OR
    box_type IS NULL OR box_type = '' OR
    category IS NULL OR category = '' OR
    dimensions IS NULL OR dimensions = '' OR
    current_occupancy IS NULL OR current_occupancy = '' OR
    mx_status IS NULL OR mx_status = '';

-- 1) Удаляем редко полезные/устаревшие индексы (если были созданы ранее).
DROP INDEX IF EXISTS idx_floor;
DROP INDEX IF EXISTS idx_location;
DROP INDEX IF EXISTS idx_storage_type;
DROP INDEX IF EXISTS idx_category;
DROP INDEX IF EXISTS idx_mx_code;

-- 2) Индексы под реальные запросы приложения.
-- Поиск и JOIN по нормализованному МХ-коду:
CREATE INDEX IF NOT EXISTS idx_mx_code_norm
    ON warehouse_places ((UPPER(BTRIM(mx_code))));

-- Часто полезно для выборок по складу + коду МХ:
CREATE INDEX IF NOT EXISTS idx_warehouse_places_wh_id_mx_id
    ON warehouse_places (wh_id, mx_id);

-- Для быстрой сортировки/выборок по обновлённым записям:
CREATE INDEX IF NOT EXISTS idx_warehouse_places_updated_at
    ON warehouse_places (updated_at DESC);

ANALYZE warehouse_places;

COMMIT;

-- 3) Материализованное представление (узкое, "рабочее").
-- Можно использовать в аналитике/отчётах вместо тяжёлой таблицы.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_warehouse_places_compact AS
SELECT
    mx_id,
    mx_code,
    UPPER(BTRIM(mx_code)) AS mx_code_norm,
    wh_id,
    warehouse_name,
    floor,
    row_num,
    section,
    shelf,
    cell,
    storage_type,
    box_type,
    category,
    dimensions,
    current_volume,
    current_occupancy,
    mx_status,
    updated_at
FROM warehouse_places
WITH NO DATA;

-- Индексы на mat view (нужны для CONCURRENTLY refresh и поиска).
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_warehouse_places_compact_mx_id
    ON mv_warehouse_places_compact (mx_id);
CREATE INDEX IF NOT EXISTS idx_mv_warehouse_places_compact_mx_code_norm
    ON mv_warehouse_places_compact (mx_code_norm);
CREATE INDEX IF NOT EXISTS idx_mv_warehouse_places_compact_wh_id
    ON mv_warehouse_places_compact (wh_id);

-- Первый прогрев данных:
REFRESH MATERIALIZED VIEW mv_warehouse_places_compact;

-- 4) Функция обновления mat view (удобно звать из cron/pg_cron).
CREATE OR REPLACE FUNCTION refresh_mv_warehouse_places_compact()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Для CONCURRENTLY обязателен уникальный индекс (создан выше).
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_warehouse_places_compact;
EXCEPTION
    WHEN feature_not_supported THEN
        -- fallback, если CONCURRENTLY недоступен
        REFRESH MATERIALIZED VIEW mv_warehouse_places_compact;
END;
$$;

-- 5) Поиск потенциально неиспользуемых колонок.
-- Этот SELECT покажет кандидатов, которых нет в whitelist приложения.
SELECT column_name AS candidate_to_drop
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'warehouse_places'
  AND column_name NOT IN (
      'mx_id', 'mx_code', 'floor', 'row_num', 'section', 'shelf', 'cell',
      'storage_type', 'category', 'dimensions', 'wh_id', 'warehouse_name',
      'box_type', 'current_volume', 'current_occupancy', 'mx_status',
      'updated_at', 'created_at'
  )
ORDER BY column_name;

-- 6) Опциональный DROP "лишних" колонок (ВЫКЛЮЧЕН ПО УМОЛЧАНИЮ).
-- Перед включением:
--   1) Сделайте backup.
--   2) Проверьте output SELECT выше.
--   3) Убедитесь, что эти поля не нужны ETL/BI/внешним скриптам.
--
-- DO $$
-- DECLARE
--     r record;
-- BEGIN
--     FOR r IN
--         SELECT column_name
--         FROM information_schema.columns
--         WHERE table_schema = 'public'
--           AND table_name = 'warehouse_places'
--           AND column_name NOT IN (
--               'mx_id', 'mx_code', 'floor', 'row_num', 'section', 'shelf', 'cell',
--               'storage_type', 'category', 'dimensions', 'wh_id', 'warehouse_name',
--               'box_type', 'current_volume', 'current_occupancy', 'mx_status',
--               'updated_at', 'created_at'
--           )
--     LOOP
--         EXECUTE format('ALTER TABLE warehouse_places DROP COLUMN IF EXISTS %I', r.column_name);
--     END LOOP;
-- END $$;

-- 7) Итоговые рекомендации по сжатию места:
--    VACUUM (ANALYZE) warehouse_places;
--    -- VACUUM FULL warehouse_places;  -- только в окно обслуживания (эксклюзивный lock)


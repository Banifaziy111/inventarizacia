-- Полная очистка warehouse_places
-- ВНИМАНИЕ: после TRUNCATE приложение не сможет получать данные МХ/плейсов
-- пока ты не запустишь загрузку (import_new_mx_data.py).
--
-- Запуск:
--   psql "$DATABASE_URL" -f truncate_warehouse_places.sql

BEGIN;

-- 1) Сколько строк было
SELECT COUNT(*) AS cnt_before FROM warehouse_places;

-- 2) Полное удаление
TRUNCATE TABLE warehouse_places;

-- 3) Сколько стало
SELECT COUNT(*) AS cnt_after FROM warehouse_places;

COMMIT;


-- db_cleanup.sql  —  ручная оптимизация БД
-- Запуск: psql "$DATABASE_URL" -f db_cleanup.sql
-- Или вставить в любую SQL-консоль (DBeaver, pgAdmin, psql).

BEGIN;

-- 1. Удалить файлы отчётов старше 30 дней (метаданные сохраняются)
UPDATE reports
SET file_data = NULL
WHERE created_at < NOW() - INTERVAL '30 days'
  AND file_data IS NOT NULL;

-- 2. Удалить строки отчётов старше 90 дней полностью
DELETE FROM reports
WHERE created_at < NOW() - INTERVAL '90 days';

-- 3. Обнулить бинарные фото в inventory_results старше 60 дней
--    (фото в inventory_result_photos остаются, они нужны при экспорте)
UPDATE inventory_results
SET photo_data = NULL, photo_filename = NULL
WHERE created_at < NOW() - INTERVAL '60 days'
  AND photo_data IS NOT NULL;

-- 4. Удалить фото из inventory_result_photos старше 60 дней
DELETE FROM inventory_result_photos
WHERE created_at < NOW() - INTERVAL '60 days';

-- 5. Удалить результаты инвентаризации старше 180 дней
--    ОСТОРОЖНО: закомментировано по умолчанию, раскомментируйте если нужно
-- DELETE FROM inventory_results
-- WHERE created_at < NOW() - INTERVAL '180 days';

-- 6. Составной индекс (если ещё не создан)
CREATE INDEX IF NOT EXISTS idx_inventory_results_badge_date
    ON inventory_results(badge, created_at DESC);

-- 7. Функциональный индекс под поиск по mx_code (UPPER TRIM)
CREATE INDEX IF NOT EXISTS idx_mx_code_norm
    ON warehouse_places ((UPPER(TRIM(mx_code))));

COMMIT;

-- 8. Обновить статистику планировщика
ANALYZE inventory_results;
ANALYZE inventory_result_photos;
ANALYZE reports;
ANALYZE warehouse_places;

-- 9. VACUUM: освободить физическое место после массового DELETE/UPDATE
--    VACUUM FULL берёт эксклюзивную блокировку — запускать в окно обслуживания!
VACUUM (ANALYZE) inventory_results;
VACUUM (ANALYZE) inventory_result_photos;
VACUUM (ANALYZE) reports;

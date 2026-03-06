-- Колонка «Статус МХ»: Активно, есть товары / Активно, нет товаров (из перечня МХ).
-- При первом запросе места приложение создаёт колонку само; скрипт нужен при ручной миграции.

ALTER TABLE warehouse_places
ADD COLUMN IF NOT EXISTS mx_status VARCHAR(150);

COMMENT ON COLUMN warehouse_places.mx_status IS 'Статус МХ из перечня: Активно, есть товары; Активно, нет товаров';

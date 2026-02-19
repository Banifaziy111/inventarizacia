-- Добавление недостающих полей для совместимости с app.py

-- Добавляем поля для инвентаризации
ALTER TABLE warehouse_places 
ADD COLUMN IF NOT EXISTS qty_shk INTEGER DEFAULT 0;

ALTER TABLE warehouse_places 
ADD COLUMN IF NOT EXISTS plt_id VARCHAR(50);

-- Создаем представление (view) для обратной совместимости со старыми запросами
CREATE OR REPLACE VIEW warehouse_places_legacy AS
SELECT 
    mx_id as place_cod,
    mx_code as place_name,
    qty_shk,
    plt_id,
    updated_at,
    -- Все остальные поля
    floor,
    row_num,
    code,
    section,
    shelf,
    number,
    cell,
    number_2,
    storage_type,
    category,
    size_group,
    dimensions,
    wh_id,
    warehouse_name,
    box_type,
    current_volume,
    current_occupancy,
    photo_fixation,
    location_stat_code,
    created_at
FROM warehouse_places;

-- Создаем индексы для новых полей
CREATE INDEX IF NOT EXISTS idx_qty_shk ON warehouse_places(qty_shk);
CREATE INDEX IF NOT EXISTS idx_plt_id ON warehouse_places(plt_id);

COMMENT ON COLUMN warehouse_places.qty_shk IS 'Количество ШК (штрих-кодов) для инвентаризации';
COMMENT ON COLUMN warehouse_places.plt_id IS 'ID паллеты';

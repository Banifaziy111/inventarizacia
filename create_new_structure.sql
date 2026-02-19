-- Создание новой структуры таблицы warehouse_places
-- Согласно формату из изображения

-- Удаляем старую таблицу (если нужно сохранить данные - сделать бэкап)
DROP TABLE IF EXISTS warehouse_places CASCADE;

-- Создаем новую таблицу с обновленной структурой
CREATE TABLE warehouse_places (
    -- Основные идентификаторы
    mx_id BIGINT PRIMARY KEY,                    -- Id МХ (уникальный идентификатор)
    mx_code VARCHAR(50) NOT NULL,                -- Наименование МХ (например: 36.06.01.02.01.01)
    
    -- Адресация склада
    floor INTEGER,                                -- Этаж
    row_num INTEGER,                              -- Ряд
    code INTEGER,                                 -- Код
    section INTEGER,                              -- Секция
    shelf INTEGER,                                -- Номер полки
    number INTEGER,                               -- Номер
    cell INTEGER,                                 -- Номер ячейки
    number_2 INTEGER,                             -- Номер (второй)
    
    -- Характеристики хранения
    storage_type VARCHAR(255),                    -- Тип хранения (Короб 590/285/290 2ЕХ пластик)
    category VARCHAR(255),                        -- Категория (ОДЕЖДА)
    size_group VARCHAR(255),                      -- Размерная группа (Стандарт)
    dimensions VARCHAR(50),                       -- Размеры (590x285x290)
    
    -- Дополнительная информация
    wh_id INTEGER,                                -- ID склада
    warehouse_name VARCHAR(255),                  -- Название склада
    box_type VARCHAR(255),                        -- Короба МХ
    current_volume DECIMAL(10,2),                 -- Текущий объем МХ
    current_occupancy VARCHAR(255),               -- Текущая заполненность
    photo_fixation VARCHAR(10),                   -- Фото-фиксация (Да/Нет)
    location_stat_code DECIMAL(10,2),             -- Стат код локации
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска
CREATE INDEX idx_mx_code ON warehouse_places(mx_code);
CREATE INDEX idx_wh_id ON warehouse_places(wh_id);
CREATE INDEX idx_floor ON warehouse_places(floor);
CREATE INDEX idx_location ON warehouse_places(floor, row_num, section, shelf);
CREATE INDEX idx_storage_type ON warehouse_places(storage_type);
CREATE INDEX idx_category ON warehouse_places(category);

-- Комментарии к таблице
COMMENT ON TABLE warehouse_places IS 'Справочник складских мест (МХ) с полной адресацией';
COMMENT ON COLUMN warehouse_places.mx_id IS 'Уникальный ID места хранения';
COMMENT ON COLUMN warehouse_places.mx_code IS 'Код МХ (например: 36.06.01.02.01.01)';
COMMENT ON COLUMN warehouse_places.floor IS 'Этаж склада';
COMMENT ON COLUMN warehouse_places.storage_type IS 'Тип хранения (размер короба)';
COMMENT ON COLUMN warehouse_places.category IS 'Категория товара';
COMMENT ON COLUMN warehouse_places.size_group IS 'Размерная группа';
COMMENT ON COLUMN warehouse_places.dimensions IS 'Габариты МХ';

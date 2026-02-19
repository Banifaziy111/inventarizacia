-- Вспомогательный SQL-скрипт для таблицы дополнительных фото (на случай ручного применения)
CREATE TABLE IF NOT EXISTS inventory_result_photos (
    photo_id SERIAL PRIMARY KEY,
    result_id INTEGER NOT NULL REFERENCES inventory_results(result_id) ON DELETE CASCADE,
    badge VARCHAR(100) NOT NULL,
    place_cod BIGINT,
    photo_data BYTEA NOT NULL,
    photo_filename VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_inventory_result_photos_result 
    ON inventory_result_photos(result_id);

CREATE INDEX IF NOT EXISTS idx_inventory_result_photos_place 
    ON inventory_result_photos(place_cod);



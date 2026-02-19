-- Добавление полей для причины расхождения

ALTER TABLE inventory_results 
ADD COLUMN IF NOT EXISTS discrepancy_reason VARCHAR(255),
ADD COLUMN IF NOT EXISTS comment TEXT;

COMMENT ON COLUMN inventory_results.discrepancy_reason IS 'Причина расхождения (неправильный размер, отсутствует, сломан и т.д.)';
COMMENT ON COLUMN inventory_results.comment IS 'Дополнительный комментарий сотрудника';

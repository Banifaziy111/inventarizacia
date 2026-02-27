-- Очистка всех записей о пользователях и результатах инвентаризации.
-- Справочники (warehouse_places и т.п.) не трогаем.
-- Запуск: psql -U user -d botdb -f clear_all_user_data.sql
-- Или выполнить вручную в порядке ниже.

BEGIN;

-- Фото к результатам (дочерняя таблица)
DELETE FROM inventory_result_photos;

-- Результаты сканирования (инвентаризация)
DELETE FROM inventory_results;

-- Сессии пользователей
DELETE FROM user_sessions;

-- Сохранённые отчёты (админка)
DELETE FROM reports;

-- Активные задания (зоны)
DELETE FROM active_tasks;

-- Ревизии качества
DELETE FROM quality_reviews;

-- Тикеты/инциденты
DELETE FROM tickets;

-- Отметки «в работе» / «исправлено» по складам
DELETE FROM repaired_places;

COMMIT;

-- После выполнения: статистика, история сканов, отчёты и тикеты будут пустыми.
-- Данные по местам хранения (warehouse_places) не удаляются.

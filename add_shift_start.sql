-- Граница смены по сотруднику: после «Новая смена» дубликат МХ в рамках смены блокируется.
-- Запустить один раз на БД.

CREATE TABLE IF NOT EXISTS shift_start (
    badge TEXT NOT NULL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT (NOW())
);

COMMENT ON TABLE shift_start IS 'Время начала текущей смены по бэйджу; обновляется при нажатии «Новая смена»';

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт очистки всех пользовательских данных: результаты инвентаризации,
сессии, отчёты, задания, тикеты, отметки по складам.
Справочники (warehouse_places) не трогаем.
Запуск: python clear_user_data.py
Использует переменные окружения из .env (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD).
"""
import os
import sys

from dotenv import load_dotenv
load_dotenv()

# Подключаем конфиг и get_db из app
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper.database import DatabaseConfig
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = DatabaseConfig(
    host=os.environ.get("DB_HOST", "31.207.77.167"),
    port=int(os.environ.get("DB_PORT", "5432")),
    database=os.environ.get("DB_NAME", "botdb"),
    user=os.environ.get("DB_USER", "aperepechkin"),
    password=os.environ.get("DB_PASSWORD", "password"),
)


def main():
    print("Будут удалены: результаты инвентаризации, сессии, отчёты, задания, тикеты, отметки по складам.")
    reply = input("Продолжить? (введите yes): ").strip().lower()
    if reply != "yes":
        print("Отменено.")
        sys.exit(0)
    print("Подключение к БД...")
    conn = psycopg2.connect(
        host=DB_CONFIG.host,
        port=DB_CONFIG.port,
        database=DB_CONFIG.database,
        user=DB_CONFIG.user,
        password=DB_CONFIG.password,
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = False
    cur = conn.cursor()

    tables = [
        ("inventory_result_photos", "Фото к результатам"),
        ("inventory_results", "Результаты инвентаризации"),
        ("user_sessions", "Сессии пользователей"),
        ("reports", "Сохранённые отчёты"),
        ("active_tasks", "Активные задания"),
        ("quality_reviews", "Ревизии качества"),
        ("tickets", "Тикеты"),
        ("repaired_places", "Отметки по складам (в работе/исправлено)"),
    ]

    try:
        for table, label in tables:
            cur.execute(f"DELETE FROM {table}")
            n = cur.rowcount
            print(f"  {label}: удалено строк {n}")
        conn.commit()
        print("Готово. Все пользовательские данные очищены.")
    except Exception as e:
        conn.rollback()
        print(f"Ошибка: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

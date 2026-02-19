"""
ОСНОВНОЙ скрипт загрузки МХ в БД.
Читает все ZIP-архивы из папки (по умолчанию archives/), распаковывает и загружает CSV в БД.

Файл "Вместимость и заполненность" НЕ требуется.
При скане пользователю отображаются: этаж, ряд, секция.
"""

import os
import zipfile
import csv
import re
import sys
from pathlib import Path
from typing import Dict, Generator, List, Optional
import psycopg2

from dotenv import load_dotenv
load_dotenv()
from psycopg2.extras import execute_values
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Папка с ZIP-архивами (все архивы из неё будут обработаны)
ARCHIVES_DIR = Path("archives")

# Размер чанка для потоковой загрузки: читаем CSV и вставляем в БД пачками
BATCH_SIZE = 50_000
# Размер пачки в одном INSERT (execute_values) — больше = меньше круг-трипов к БД
INSERT_PAGE_SIZE = 15_000

# Конфигурация БД (из .env или переменных окружения)
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', '31.207.77.167'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'database': os.environ.get('DB_NAME', 'botdb'),
    'user': os.environ.get('DB_USER', 'aperepechkin'),
    'password': os.environ.get('DB_PASSWORD', 'password'),
}


def extract_zip_files(archives_dir: Optional[Path] = None) -> List[Path]:
    """
    Распаковать все ZIP-архивы из папки и вернуть список путей к CSV.
    Каждый архив распаковывается в свою подпапку temp_extracted/<имя_архива>/.
    """
    archives_dir = archives_dir or ARCHIVES_DIR
    if not archives_dir.is_dir():
        logger.error("Папка с архивами не найдена: %s. Создайте папку и положите туда ZIP-файлы.", archives_dir.resolve())
        return []

    zip_files = sorted(archives_dir.glob("*.zip"))
    if not zip_files:
        logger.error("В папке %s нет ZIP-файлов!", archives_dir.resolve())
        return []

    logger.info("Найдено архивов: %s", len(zip_files))
    extract_base = Path("temp_extracted")
    extract_base.mkdir(exist_ok=True)
    all_csv: List[Path] = []

    for zip_path in zip_files:
        extract_dir = extract_base / zip_path.stem
        extract_dir.mkdir(exist_ok=True)
        logger.info("Распаковка %s ...", zip_path.name)
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
        except zipfile.BadZipFile as e:
            logger.warning("Пропуск архива %s: %s", zip_path.name, e)
            continue
        csv_in_archive = list(extract_dir.glob("**/*.csv"))
        all_csv.extend(csv_in_archive)
        logger.info("  CSV в архиве: %s", len(csv_in_archive))

    logger.info("Всего CSV файлов для обработки: %s", len(all_csv))
    return all_csv


def parse_mx_code(mx_code: str) -> Dict[str, Optional[int]]:
    """
    Парсинг кода МХ формата: Ц6.06.01.02.01.01
    Возвращает словарь с компонентами адреса.
    
    Структура:
    - Позиция 1: Префикс склада (Ц6 или 36 или др.) - строка
    - Позиция 2: Этаж (06)
    - Позиция 3: Ряд (01)
    - Позиция 4: Секция (02)
    - Позиция 5: Полка (01)
    - Позиция 6: Ячейка (01)
    """
    parts = mx_code.split('.')
    
    result = {
        'code': None,  # Оставляем как строку
        'floor': None,
        'row_num': None,
        'section': None,
        'shelf': None,
        'cell': None
    }
    
    try:
        # Первая часть может быть "Ц6", "36" и т.д. - оставляем как строку
        if len(parts) >= 1:
            # Извлекаем только цифры из первой части
            code_str = parts[0]
            # Извлекаем цифры
            digits = ''.join(filter(str.isdigit, code_str))
            if digits:
                result['code'] = int(digits)
        
        # Остальные части - цифры
        if len(parts) >= 2:
            result['floor'] = int(parts[1])
        if len(parts) >= 3:
            result['row_num'] = int(parts[2])
        if len(parts) >= 4:
            result['section'] = int(parts[3])
        if len(parts) >= 5:
            result['shelf'] = int(parts[4])
        if len(parts) >= 6:
            result['cell'] = int(parts[5])
    except (ValueError, IndexError):
        # Убираем логирование каждой ошибки - их слишком много
        pass
    
    return result


def process_csv_row(row: Dict) -> Optional[Dict]:
    """
    Обработка строки CSV и преобразование в формат для БД.
    
    Входные колонки CSV:
    - Id МХ
    - Наименование МХ
    - WH ID
    - Этаж
    - Ряд
    - Секция
    - Номер полки
    - Номер ячейки
    - Короба МХ
    - Текущий объем МХ
    - Текущая заполненая вместимость МХ МХ ячейки
    - Фото-фиксация (кейс превышен или открытое МХ последнии 30 дней)
    - Стат код локации
    """
    try:
        mx_id = int(row.get('Id МХ', 0))
        mx_code = row.get('Наименование МХ', '').strip()
        
        if not mx_id or not mx_code:
            return None
        
        # Парсим компоненты из mx_code
        parsed = parse_mx_code(mx_code)
        
        # Используем данные из CSV, если они есть
        wh_id = int(row.get('WH ID', 0)) if row.get('WH ID') else None
        
        # Извлекаем floor из столбца, если есть
        floor_from_csv = None
        etaj = row.get('Этаж', '')
        if etaj and isinstance(etaj, str):
            # Извлекаем число из строки типа "Центртерминал 6"
            match = re.search(r'\d+', etaj)
            if match:
                floor_from_csv = int(match.group())
        
        # Преобразуем числовые поля
        def safe_int(value):
            if value == '' or value is None:
                return None
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None
        
        def safe_float(value):
            if value == '' or value is None:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        row_num = safe_int(row.get('Ряд'))
        section = safe_int(row.get('Секция'))
        shelf = safe_int(row.get('Номер полки'))
        cell = safe_int(row.get('Номер ячейки'))
        
        # Формируем запись
        record = {
            'mx_id': mx_id,
            'mx_code': mx_code,
            'floor': floor_from_csv or parsed['floor'],
            'row_num': row_num or parsed['row_num'],
            'code': parsed['code'],
            'section': section or parsed['section'],
            'shelf': shelf or parsed['shelf'],
            'number': None,  # Нет в CSV
            'cell': cell or parsed['cell'],
            'number_2': None,  # Нет в CSV
            'storage_type': row.get('Короба МХ', '').strip() or None,
            'category': None,  # Нужно извлечь из других данных
            'size_group': None,  # Нужно извлечь из других данных
            'dimensions': None,  # Нужно извлечь из других данных
            'wh_id': wh_id,
            'warehouse_name': row.get('Этаж', '').strip() or None,
            'box_type': row.get('Короба МХ', '').strip() or None,
            'current_volume': safe_float(row.get('Текущий объем МХ')),
            'current_occupancy': row.get('Текущая заполненая вместимость МХ МХ ячейки', '').strip() or None,
            'photo_fixation': row.get('Фото-фиксация (кейс превышен или открытое МХ последнии 30 дней)', '').strip() or None,
            'location_stat_code': safe_float(row.get('Стат код локации'))
        }
        
        return record
    
    except Exception as e:
        logger.error(f"Ошибка обработки строки: {e}, данные: {row}")
        return None


def read_csv_with_encoding(file_path: Path, encoding: str = 'cp1251') -> List[Dict]:
    """Чтение CSV файла с указанной кодировкой (все записи в память)."""
    records = []
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                record = process_csv_row(row)
                if record:
                    records.append(record)
        logger.info(f"Обработано {len(records)} записей из {file_path.name}")
    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path.name}: {e}")
    return records


def read_csv_stream(
    file_path: Path,
    batch_size: int = BATCH_SIZE,
    encoding: str = 'cp1251',
) -> Generator[List[Dict], None, None]:
    """
    Потоковое чтение CSV: выдаёт чанки записей по batch_size.
    Не держит весь файл в памяти.
    """
    batch: List[Dict] = []
    rows_read = 0
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows_read += 1
                record = process_csv_row(row)
                if record:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        if rows_read % 100_000 == 0 or rows_read <= batch_size:
                            logger.info(f"  {file_path.name}: прочитано {rows_read} строк...")
                        yield batch
                        batch = []
        if batch:
            yield batch
        logger.info(f"  {file_path.name}: прочитано {rows_read} строк")
    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path.name}: {e}")
        if batch:
            yield batch


def insert_batch(
    conn,
    records: List[Dict],
    page_size: int = INSERT_PAGE_SIZE,
    commit_after: bool = False,
) -> int:
    """Вставка одной пачки записей в БД. Если commit_after=False, коммит не делается (одна транзакция на всю загрузку — быстрее)."""
    if not records:
        return 0
    insert_query = """
        INSERT INTO warehouse_places (
            mx_id, mx_code, floor, row_num, code, section, shelf, number, cell, number_2,
            storage_type, category, size_group, dimensions,
            wh_id, warehouse_name, box_type, current_volume, current_occupancy,
            photo_fixation, location_stat_code
        ) VALUES %s
        ON CONFLICT (mx_id) DO UPDATE SET
            mx_code = EXCLUDED.mx_code,
            floor = EXCLUDED.floor,
            row_num = EXCLUDED.row_num,
            code = EXCLUDED.code,
            section = EXCLUDED.section,
            shelf = EXCLUDED.shelf,
            cell = EXCLUDED.cell,
            storage_type = EXCLUDED.storage_type,
            wh_id = EXCLUDED.wh_id,
            warehouse_name = EXCLUDED.warehouse_name,
            box_type = EXCLUDED.box_type,
            current_volume = EXCLUDED.current_volume,
            current_occupancy = EXCLUDED.current_occupancy,
            photo_fixation = EXCLUDED.photo_fixation,
            location_stat_code = EXCLUDED.location_stat_code,
            updated_at = CURRENT_TIMESTAMP
    """
    values = [
        (
            r['mx_id'], r['mx_code'], r['floor'], r['row_num'], r['code'],
            r['section'], r['shelf'], r['number'], r['cell'], r['number_2'],
            r['storage_type'], r['category'], r['size_group'], r['dimensions'],
            r['wh_id'], r['warehouse_name'], r['box_type'], r['current_volume'],
            r['current_occupancy'], r['photo_fixation'], r['location_stat_code']
        )
        for r in records
    ]
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values, page_size=page_size)
    if commit_after:
        conn.commit()
    return len(records)


def main():
    """Главная функция. Можно передать путь к папке с архивами: python import_new_mx_data.py [папка]"""
    archives_dir = ARCHIVES_DIR
    if len(sys.argv) > 1:
        archives_dir = Path(sys.argv[1])
        logger.info("Папка с архивами: %s", archives_dir.resolve())

    logger.info("="*80)
    logger.info("НАЧАЛО МИГРАЦИИ ДАННЫХ МХ")
    logger.info("="*80)

    # 1. Распаковка всех ZIP из папки и поиск CSV файлов
    csv_files = extract_zip_files(archives_dir)
    if not csv_files:
        logger.error("CSV файлы не найдены!")
        return
    
    # 2. Подключение к БД
    logger.info("Подключение к базе данных...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Одна транзакция на всю загрузку (без commit после каждой пачки) — сильно ускоряет.
        total_inserted = 0
        for csv_file in csv_files:
            logger.info("Обработка файла: %s", csv_file.name)
            for batch in read_csv_stream(csv_file, batch_size=BATCH_SIZE):
                n = insert_batch(conn, batch, commit_after=False)
                total_inserted += n
                if total_inserted % 100_000 == 0 and total_inserted > 0:
                    logger.info("  В БД загружено записей: %s", total_inserted)

        conn.commit()
        logger.info("Всего вставлено/обновлено записей: %s", total_inserted)

        # Статистика
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM warehouse_places")
            total_count = cur.fetchone()[0]
            logger.info("Всего записей в таблице warehouse_places: %s", total_count)
        
        logger.info("="*80)
        logger.info("МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        logger.info("="*80)
    
    except Exception as e:
        logger.error(f"Ошибка миграции: {e}")
        conn.rollback()
        raise
    
    finally:
        conn.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tmdb_db_cleanup_v6_final.py

Полная версия очистки:
1. Удаление записей:
   - Без описания (overview).
   - Не на русском языке.
   - Из будущего (> текущий год).
   - Старые (< 1980 года).
2. Удаление записей без файла постера.
3. Обновление стран (ISO -> Имя).
4. Перестройка таблицы (добавление id_slug) + Верификация.
5. (NEW) Очистка папки: удаление файлов постеров, которых нет в БД.
"""

import sqlite3
import os
import re
import logging
from pathlib import Path
from datetime import date
import sys

# --- Зависимости ---
try:
    import pycountry
except ImportError:
    print("Ошибка: библиотека pycountry не найдена. pip install pycountry")
    raise SystemExit(1)

try:
    from slugify import slugify
except Exception:
    print("Ошибка: библиотека python-slugify не найдена. pip install python-slugify")
    raise SystemExit(1)


# --- НАСТРОЙКИ ---
DB_PATH = Path("tmdb_data/tmdb_minimal_no_original.db")
POSTERS_DIR = Path("/files/posters")  # Папка, которую будем чистить в конце
LOG_FILE = "db_cleanup_strict_with_verify.log"
TABLE_NAME = "items_minimal"

# Минимальный год (все что меньше - удаляем из БД)
MIN_YEAR = 1980

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Словарь ISO->Русский
COUNTRY_CODE_MAP_RU = {
    "US": "США", "CA": "Канада", "MX": "Мексика",
    "RU": "Россия", "FR": "Франция", "DE": "Германия", "GB": "Великобритания",
    "IT": "Италия", "ES": "Испания", "NL": "Нидерланды", "BE": "Бельгия",
    "SE": "Швеция", "NO": "Норвегия", "DK": "Дания", "FI": "Финляндия",
    "PL": "Польша", "CZ": "Чехия", "HU": "Венгрия", "AT": "Австрия",
    "CH": "Швейцария", "IE": "Ирландия", "PT": "Португалия", "GR": "Греция",
    "RO": "Румыния", "UA": "Украина", "BY": "Беларусь", "KZ": "Казахстан",
    "BG": "Болгария", "RS": "Сербия", "HR": "Хорватия", "SI": "Словения",
    "SK": "Словакия", "LT": "Литва", "LV": "Латвия", "EE": "Эстония",
    "IS": "Исландия", "LU": "Люксембург", "MT": "Мальта",
    "CN": "Китай", "JP": "Япония", "IN": "Индия", "KR": "Южная Корея",
    "HK": "Гонконг", "TR": "Турция", "IL": "Израиль", "TH": "Таиланд",
    "TW": "Тайвань", "IR": "Иран", "ID": "Индонезия", "PH": "Филиппины",
    "MY": "Малайзия", "SG": "Сингапур", "VN": "Вьетнам", "AE": "ОАЭ",
    "BR": "Бразилия", "AR": "Аргентина", "CL": "Чили", "CO": "Колумбия",
    "AU": "Австралия", "NZ": "Новая Зеландия",
    "ZA": "ЮАР", "EG": "Египет", "MA": "Марокко", "NG": "Нигерия",
}

# --- Вспомогательные функции ---

def is_russian(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    return bool(re.search('[а-яА-ЯёЁ]', text))

def get_country_name(iso_code: str) -> str:
    if not iso_code: return iso_code
    code = iso_code.strip().upper()
    if code in COUNTRY_CODE_MAP_RU:
        return COUNTRY_CODE_MAP_RU[code]
    try:
        c = pycountry.countries.get(alpha_2=code)
        if c: return c.name
    except: pass
    return code

def generate_id_slug_from_row(item_id, title):
    tid = str(item_id)
    t = (title or "").strip()
    if not t: return tid
    try:
        s = slugify(t)
    except: s = ""
    return f"{tid}-{s}" if s else tid

def remove_poster_file(poster_path_value):
    """Удаляет конкретный файл (используется при удалении записи из БД)."""
    if not poster_path_value: return False
    p = Path(poster_path_value)
    if not p.is_absolute():
        p = POSTERS_DIR / poster_path_value
    try:
        if p.exists():
            p.unlink()
            return True
    except: pass
    return False


# --- ВОЛНЫ ОЧИСТКИ ---

def wave_1_delete_bad_content(cursor, current_year):
    """
    Удаляет записи:
    1. Нет описания (Overview пусто).
    2. Не русский язык.
    3. Год > текущего.
    4. Год < MIN_YEAR.
    """
    logging.info(f"Поиск кандидатов на удаление (Описание, Язык, Будущее, Старые < {MIN_YEAR})...")
    
    cursor.execute(f"SELECT id, title, year, local_poster_path, overview FROM {TABLE_NAME}")
    rows = cursor.fetchall()

    ids_to_delete = []
    
    c_over = 0
    c_lang = 0
    c_fut = 0
    c_old = 0
    
    for row in rows:
        rid, title, year_val, poster, overview = row
        
        # 1. Описание
        if not overview or str(overview).strip() == "":
            remove_poster_file(poster)
            ids_to_delete.append(rid)
            c_over += 1
            continue

        # 2. Язык
        if not is_russian(title):
            remove_poster_file(poster)
            ids_to_delete.append(rid)
            c_lang += 1
            continue
        
        # Год
        try:
            y = int(year_val) if year_val else 0
        except:
            m = re.search(r'\d{4}', str(year_val))
            y = int(m.group(0)) if m else 0
        
        # 3. Будущее
        if y > current_year:
            remove_poster_file(poster)
            ids_to_delete.append(rid)
            c_fut += 1
            continue

        # 4. Старье
        if y > 0 and y < MIN_YEAR:
            remove_poster_file(poster)
            ids_to_delete.append(rid)
            c_old += 1
            continue

    if ids_to_delete:
        logging.info(f"Удаление {len(ids_to_delete)} записей...")
        cursor.executemany(f"DELETE FROM {TABLE_NAME} WHERE id=?", [(i,) for i in ids_to_delete])
    
    return c_over, c_lang, c_fut, c_old


def wave_2_delete_without_posters(cursor):
    logging.info("Поиск записей без файлов постеров...")
    cursor.execute(f"SELECT id, local_poster_path FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    
    ids_to_delete = []
    for rid, poster in rows:
        bad = False
        if not poster or not poster.strip():
            bad = True
        else:
            p = Path(poster)
            if not p.is_absolute(): p = POSTERS_DIR / poster
            if not p.exists(): bad = True
        
        if bad: ids_to_delete.append(rid)

    if ids_to_delete:
        logging.info(f"Удаление {len(ids_to_delete)} записей без постеров...")
        cursor.executemany(f"DELETE FROM {TABLE_NAME} WHERE id=?", [(i,) for i in ids_to_delete])
        
    return len(ids_to_delete)


def wave_3_update_countries(cursor):
    logging.info("Обновление стран...")
    cursor.execute(f"SELECT id, production_countries FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    
    upd = 0
    for rid, c_str in rows:
        if not c_str: continue
        parts = [c.strip() for c in c_str.split(',') if c.strip()]
        new_parts = []
        chg = False
        for item in parts:
            if len(item) == 2 and item.isupper():
                nm = get_country_name(item)
                new_parts.append(nm)
                if nm != item: chg = True
            else:
                new_parts.append(item)
        
        if chg:
            nv = ", ".join(new_parts)
            cursor.execute(f"UPDATE {TABLE_NAME} SET production_countries=? WHERE id=?", (nv, rid))
            upd += 1
    return upd


def wave_4_rebuild_table_add_id_slug(conn):
    cursor = conn.cursor()
    backup_table = f"{TABLE_NAME}_backup"
    
    logging.info(f"Бэкап: {TABLE_NAME} -> {backup_table}")
    cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
    cursor.execute(f"ALTER TABLE {TABLE_NAME} RENAME TO {backup_table}")
    
    cursor.execute(f"PRAGMA table_info({backup_table})")
    columns_info = cursor.fetchall()
    col_names = [x[1] for x in columns_info]
    
    if 'id' not in col_names or 'title' not in col_names:
        logging.error("Нет полей id или title!")
        return False

    new_col_defs = []
    pk_cols = [] 
    for info in columns_info:
        name = info[1]
        dtype = info[2]
        pk_idx = info[5]
        if name == 'id_slug': continue
        definition = f'"{name}" {dtype}'
        new_col_defs.append(definition)
        if pk_idx > 0: pk_cols.append((pk_idx, name))

    new_col_defs.append('"id_slug" TEXT')
    if pk_cols:
        pk_cols.sort(key=lambda x: x[0])
        pk_names_list = [f'"{x[1]}"' for x in pk_cols]
        pk_constraint = f"PRIMARY KEY ({', '.join(pk_names_list)})"
        new_col_defs.append(pk_constraint)

    create_sql = f"CREATE TABLE {TABLE_NAME} ({', '.join(new_col_defs)})"
    logging.info(f"Создание таблицы: {create_sql}")
    cursor.execute(create_sql)
    
    logging.info("Перенос данных...")
    cursor.execute(f"SELECT * FROM {backup_table}")
    old_rows = cursor.fetchall()
    
    clean_cols = [c for c in col_names if c != 'id_slug']
    placeholders = ", ".join(["?"] * (len(clean_cols) + 1))
    cols_for_insert = [f'"{c}"' for c in clean_cols] + ['"id_slug"']
    insert_sql = f"INSERT INTO {TABLE_NAME} ({', '.join(cols_for_insert)}) VALUES ({placeholders})"
    
    idx_id = col_names.index('id')
    idx_title = col_names.index('title')

    cnt = 0
    for row in old_rows:
        row_data = []
        for col in clean_cols:
            idx = col_names.index(col)
            row_data.append(row[idx])
        tid = row[idx_id]
        ttl = row[idx_title]
        row_data.append(generate_id_slug_from_row(tid, ttl))
        cursor.execute(insert_sql, row_data)
        cnt += 1
        
    logging.info(f"Перенесено: {cnt}")
    return True


def verify_no_data_loss(conn, backup_table, new_table):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {backup_table}")
    b_c = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {new_table}")
    n_c = cur.fetchone()[0]
    logging.info(f"Backup: {b_c}, New: {n_c}")
    return b_c == n_c


# --- НОВАЯ ВОЛНА 5: ЧИСТКА ФАЙЛОВОЙ СИСТЕМЫ ---
def wave_5_delete_orphaned_posters(cursor):
    """
    Сканирует папку постеров. Если находит файл, которого нет в БД, удаляет его.
    """
    logging.info("ВОЛНА 5: Удаление 'сиротских' файлов постеров (которых нет в БД)...")
    
    if not POSTERS_DIR.exists():
        logging.error(f"Папка {POSTERS_DIR} не найдена! Пропускаю шаг.")
        return 0

    # 1. Собираем множество имен файлов, которые ЕСТЬ в базе
    logging.info("Сбор списка файлов из БД...")
    cursor.execute(f"SELECT local_poster_path FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    
    valid_filenames = set()
    for (path_str,) in rows:
        if path_str:
            # Берем только имя файла (на случай если там относительный путь типа /files/posters/a.jpg)
            fname = Path(path_str).name
            valid_filenames.add(fname)
            
    logging.info(f"В БД используется {len(valid_filenames)} уникальных файлов.")

    # 2. Сканируем папку и удаляем лишнее
    deleted_count = 0
    scanned_count = 0
    
    for file_path in POSTERS_DIR.iterdir():
        if file_path.is_file():
            scanned_count += 1
            # Если имя файла отсутствует в valid_filenames -> удаляем
            if file_path.name not in valid_filenames:
                try:
                    file_path.unlink()
                    deleted_count += 1
                    if deleted_count % 1000 == 0:
                        logging.info(f"Уже удалено {deleted_count} лишних файлов...")
                except Exception as e:
                    logging.error(f"Не удалось удалить {file_path}: {e}")

    logging.info(f"Сканирование завершено. Проверено файлов: {scanned_count}. Удалено лишних: {deleted_count}.")
    return deleted_count


# --- MAIN ---

def cleanup_database():
    if not DB_PATH.exists():
        print(f"Нет файла {DB_PATH}")
        return

    cy = date.today().year
    logging.info(f"СТАРТ. Год: {cy}. Удаляем старше {MIN_YEAR}")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Восстановление таблицы при сбое
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (f"{TABLE_NAME}_backup",))
        if cursor.fetchone():
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
            if not cursor.fetchone():
                logging.warning("Восстановление из бэкапа...")
                cursor.execute(f"ALTER TABLE {TABLE_NAME}_backup RENAME TO {TABLE_NAME}")
                conn.commit()

        # 1. Чистка контента (БД)
        d_over, d_lang, d_fut, d_old = wave_1_delete_bad_content(cursor, cy)
        conn.commit()
        logging.info(f"Удалено (БД): Описание={d_over}, Язык={d_lang}, Будущее={d_fut}, Старые={d_old}")
        
        # 2. Чистка битых ссылок (БД)
        d_post = wave_2_delete_without_posters(cursor)
        conn.commit()
        logging.info(f"Удалено (БД) без постеров: {d_post}")
        
        # 3. Обновление стран
        u_cnt = wave_3_update_countries(cursor)
        conn.commit()
        logging.info(f"Обновлено стран: {u_cnt}")
        
        # 4. Ребилд и верификация
        rebuild_ok = wave_4_rebuild_table_add_id_slug(conn)
        if rebuild_ok:
            conn.commit()
            if verify_no_data_loss(conn, f"{TABLE_NAME}_backup", TABLE_NAME):
                logging.info("Верификация БД успешна.")
                
                # 5. Финальная чистка файлов на диске
                # Запускаем ТОЛЬКО если база в порядке
                wave_5_delete_orphaned_posters(cursor)
                
            else:
                logging.error("ОШИБКА ВЕРИФИКАЦИИ! Чистка файлов отменена во избежание потерь.")
        else:
            logging.error("Ошибка перестройки таблицы.")

    except Exception as e:
        logging.exception(f"Global Error: {e}")
    finally:
        if conn: conn.close()
        logging.info("Конец.")

if __name__ == "__main__":
    cleanup_database()

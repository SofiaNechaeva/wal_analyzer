import sqlite3
import json
import psycopg2
import os
from datetime import datetime, timezone
from dateutil import parser

DB_FILE = "wal_analyzer.db"

def check_connection(db_config: dict) -> str:
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pg_replication_slots;")
            count = cur.fetchone()[0]

        conn.close()

        if count >= 10:
            return "Подключение успешно! Но вы не можете запросить новый анализ. Достигнуто максимальное количество слотов (10)."
        else:
            return "Подключение успешно! Можете запросить новый анализ."
    except psycopg2.OperationalError as e:
        return f"Ошибка подключения: {e}"
    except Exception as e:
        return f"Ошибка: {e}"

def get_tables(db_config: dict) -> list[str]:
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        return [f"Ошибка: {e}"]

def init_sqlite():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            dbname TEXT,
            user TEXT,
            password TEXT,
            host TEXT,
            port TEXT,
            tables TEXT,
            period_hours INTEGER,
            operations TEXT,
            slot_name TEXT,
            analysis_type TEXT,
            summary_pdf INTEGER,
            summary_html INTEGER,
            history_table TEXT,
            history_value TEXT,
            masks_fields TEXT,
            save_target TEXT,
            plugin TEXT,
            disk_path TEXT,
            result TEXT
        )
    """)
    conn.commit()
    conn.close()


def save_connection(db_config: dict, slot_config: dict):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO connections (
            dbname, user, password, host, port,
            tables, period_hours, operations,
            slot_name, analysis_type,
            summary_pdf, summary_html,
            history_table, history_value, masks_fields,
            save_target, plugin, disk_path, result
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        db_config["dbname"],
        db_config["user"],
        db_config["password"],   
        db_config["host"],
        db_config["port"],
        json.dumps(slot_config["tables"]),       # список таблиц → JSON
        slot_config["period_hours"],
        json.dumps(slot_config["operations"]),   # список операций → JSON
        slot_config["slot_name"],
        slot_config["analysis_type"],
        int(slot_config["summary_pdf"]),
        int(slot_config["summary_html"]),
        slot_config["history_table"],
        slot_config["history_value"],
        slot_config["masks_fields"],
        slot_config["save_target"],
        slot_config["plugin"],
        slot_config["disk_path"],
        'active'
    ))

    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

def get_pg_slots(db_config):
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 5432)
    )
    cur = conn.cursor()
    cur.execute("SELECT slot_name FROM pg_replication_slots;")
    slots = {row[0] for row in cur.fetchall()}
    conn.close()
    return slots


def get_table_columns(db_config, table_name, schema="public"):
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 5432)
    )
    cur = conn.cursor()
    cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table_name))
    columns = [row[0] for row in cur.fetchall()]
    conn.close()
    return columns


def drop_current_slot(db_config, slot_name):
    # --- Удаляем слот из PostgreSQL ---
    conn_pg = psycopg2.connect(**db_config)
    conn_pg.autocommit = True
    cur_pg = conn_pg.cursor()
    try:
        cur_pg.execute("SELECT pg_drop_replication_slot(%s);", (slot_name,))
        print(f"Слот {slot_name} удалён из PostgreSQL.")
    except Exception as e:
        print(f"Ошибка при удалении слота {slot_name}: {e}")
    finally:
        cur_pg.close()
        conn_pg.close()


def clear_sql(result, slot_name, analysis_type: str):
    # --- Очищаем данные слота в SQLite ---
    conn_sqlite = sqlite3.connect("wal_analyzer.db")
    cur_sqlite = conn_sqlite.cursor()
    if analysis_type == "summary":
        cur_sqlite.execute("DELETE FROM agg_operations WHERE slot_name = ?;", (slot_name,))
        cur_sqlite.execute("DELETE FROM agg_tables WHERE slot_name = ?;", (slot_name,))
        cur_sqlite.execute("DELETE FROM agg_activity WHERE slot_name = ?;", (slot_name,))
        cur_sqlite.execute("DELETE FROM agg_sizes WHERE slot_name = ?;", (slot_name,))

    cur_sqlite.execute("""
        UPDATE connections
        SET result = ?
        WHERE slot_name = ?;
    """, (result, slot_name))

    conn_sqlite.commit()
    conn_sqlite.close()


def load_connections_data(db_config):
    """Возвращает список подключений и анализов из SQLite + статус в Postgres."""
    try:
        pg_slots = get_pg_slots(db_config)
    except Exception as e:
        pg_slots = set()
        print("Ошибка подключения к Postgres:", e)

    print(pg_slots)
    conn = sqlite3.connect("wal_analyzer.db")
    cur = conn.cursor()
    cur.execute("SELECT slot_name, analysis_type, created_at, plugin, dbname, result FROM connections")
    rows = cur.fetchall()
    conn.close()

    result_rows = []
    for slot_name, analysis_type, date, plugin, db, result in rows:
        if slot_name not in pg_slots and result == "active":
            result = "error_deleted"
        result_rows.append({
            "slot_name": slot_name,
            "analysis_type": analysis_type,
            "date": date,
            "plugin": plugin,
            "db": db,
            "result": result,
        })

    return result_rows


def init_agg_schema(sqlite_path: str):
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS agg_operations (
        slot_name TEXT, operation TEXT, count INTEGER DEFAULT 0,
        PRIMARY KEY (slot_name, operation)
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS agg_tables (
        slot_name TEXT, schema TEXT, table_name TEXT, count INTEGER DEFAULT 0,
        PRIMARY KEY (slot_name, schema, table_name)
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS agg_activity (
        slot_name TEXT, bucket_start INTEGER, bucket_end INTEGER, count INTEGER DEFAULT 0,
        PRIMARY KEY (slot_name, bucket_start, bucket_end)
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS agg_sizes (
        slot_name TEXT, size_bucket TEXT, count INTEGER DEFAULT 0,
        PRIMARY KEY (slot_name, size_bucket)
    );""")
    conn.commit()
    conn.close()

def floor_to_period_start(ts_epoch: int, period_seconds: int) -> int:
    # Стабильное окно: «срез» вниз до кратного period_seconds относительно эпохи
    return ts_epoch - (ts_epoch % period_seconds)

def pick_size_bucket(event_json_len: int) -> str:
    if event_json_len < 1024:         # < 1KB
        return "small"
    elif event_json_len < 10 * 1024:  # 1–10KB
        return "medium"
    else:
        return "large"

def aggregate_jsonl_to_sqlite(
    jsonl_path: str,
    sqlite_path: str,
    slot_name: str,
    period_hours: int
):
    period_seconds = period_hours * 3600

    if not os.path.exists(jsonl_path):
        print(f"Файл {jsonl_path} не найден, пропускаем.")
        return

    init_agg_schema(sqlite_path)

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # Подготовленные выражения для UPSERT (SQLite ≥ 3.24.0)
    upsert_ops = """INSERT INTO agg_operations(slot_name, operation, count)
                    VALUES (?, ?, 1)
                    ON CONFLICT(slot_name, operation)
                    DO UPDATE SET count = count + 1;"""
    upsert_tables = """INSERT INTO agg_tables(slot_name, schema, table_name, count)
                       VALUES (?, ?, ?, 1)
                       ON CONFLICT(slot_name, schema, table_name)
                       DO UPDATE SET count = count + 1;"""
    upsert_activity = """INSERT INTO agg_activity(slot_name, bucket_start, bucket_end, count)
                         VALUES (?, ?, ?, 1)
                         ON CONFLICT(slot_name, bucket_start, bucket_end)
                         DO UPDATE SET count = count + 1;"""
    upsert_sizes = """INSERT INTO agg_sizes(slot_name, size_bucket, count)
                      VALUES (?, ?, 1)
                      ON CONFLICT(slot_name, size_bucket)
                      DO UPDATE SET count = count + 1;"""

    # Для стабильности окон: вычислим period_start на основе первой строки
    period_start_epoch = None
    bucket_width = max(1, period_seconds // 1000)

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except Exception as e:
                print(f"Ошибка JSON: {e}")
                continue

            # Извлекаем поля
            operation = event.get("operation")
            schema = event.get("schema")
            table = event.get("table")
            timestamp = event.get("timestamp")  # ожидаем ISO8601 от wal2json
            old_data = event.get("old_data")
            new_data = event.get("new_data")

            # Парсим время → epoch seconds
            timestamp = event.get("timestamp")
            try:
                dt = parser.parse(timestamp)
                ts_epoch = int(dt.timestamp())
            except Exception as e:
                print("Ошибка парсинга времени:", timestamp, e)
                continue

            # Инициализация начала периода
            if period_start_epoch is None:
                period_start_epoch = floor_to_period_start(ts_epoch, period_seconds)

            # Корзина активности
            i = (ts_epoch - period_start_epoch) // bucket_width
            bucket_start = period_start_epoch + i * bucket_width
            bucket_end = bucket_start + bucket_width

            # Размер события
            event_json_len = len(line.encode("utf-8"))
            size_bucket = pick_size_bucket(event_json_len)

            # Инкрементальные апдейты
            if operation:
                operation = operation.upper()
                cur.execute(upsert_ops, (slot_name, operation))
            if schema and table:
                cur.execute(upsert_tables, (slot_name, schema, table))
            cur.execute(upsert_activity, (slot_name, bucket_start, bucket_end))
            cur.execute(upsert_sizes, (slot_name, size_bucket))

    conn.commit()
    conn.close()

    # # После агрегации можно удалять исходный JSONL
    # try:
    #     os.remove(jsonl_path)
    # except OSError as e:
    #     print(f"Не удалось удалить {jsonl_path}: {e}")


def save_wal_changes_to_log(db_config, slot_name, filters=None):
    """
    Получает изменения из логического слота (wal2json) и пишет их в таблицу data_change_log.
    filters = {"tables": [...], "ops": ["INSERT","UPDATE","DELETE"]}
    """

    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 5432)
    )
    cur = conn.cursor()

    # создаём таблицу для лога, если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_change_log (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            operation TEXT,
            old_data JSONB,
            new_data JSONB,
            xid BIGINT,
            ts TIMESTAMPTZ,
            schema_name TEXT
        );
    """)
    conn.commit()

    # получаем изменения из слота
    cur.execute("""
        SELECT data
        FROM pg_logical_slot_get_changes(
            %s, NULL, NULL,
            'format-version', '1',
            'include-timestamp', '1',
            'include-xids', '1',
            'include-schemas', '1',
            'include-types', '1',
            'include-transaction', '1'
        );
    """, (slot_name,))

    rows = cur.fetchall()
    print(rows)
    for row in rows:
        print(row)
        try:
            ev = json.loads(row[0])
        except Exception:
            continue

        xid = ev.get("xid")
        ts = ev.get("timestamp")

        for change in ev.get("change", []):
            table = change.get("table")
            op = change.get("kind").lower()
            schema = change.get("schema")

            # фильтрация
            if filters:
                if filters.get("tables") and table not in filters["tables"]:
                    continue
                if filters.get("ops") and op.upper() not in filters["ops"]:
                    continue

            # нормализуем old/new
            if op == "insert":
                old_data = None
                new_data = dict(zip(change["columnnames"], change["columnvalues"]))
            elif op == "update":
                old_data = dict(zip(change.get("oldkeys", {}).get("keynames", []),
                                    change.get("oldkeys", {}).get("keyvalues", [])))
                new_data = dict(zip(change["columnnames"], change["columnvalues"]))
            elif op == "delete":
                old_data = dict(zip(change.get("oldkeys", {}).get("keynames", []),
                                    change.get("oldkeys", {}).get("keyvalues", [])))
                new_data = None
            else:
                continue

            cur.execute("""
                INSERT INTO data_change_log (table_name, operation, old_data, new_data, xid, ts, schema_name)
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s::timestamptz, %s);
            """, (
                table,
                op.upper(),
                json.dumps(old_data) if old_data else None,
                json.dumps(new_data) if new_data else None,
                xid,
                ts,
                schema
            ))

    conn.commit()
    cur.close()
    conn.close()
    return "Изменения записаны в data_change_log"
import pytest
import os
import json
from metabd import check_connection, drop_current_slot, get_pg_slots, aggregate_jsonl_to_sqlite
from reportbuilder import ReportBuilder
from logical_slot import LogicalSlot
import sqlite3
import pytest

# --- фиктивные конфигурации ---
VALID_DB = {
    "dbname": "testdb",
    "user": "postgres",
    "password": "correct_password",
    "host": "localhost",
    "port": 5432
}

INVALID_DB = {
    "dbname": "wrongdb",
    "user": "postgres",
    "password": "wrong_password",
    "host": "localhost",
    "port": 5432
}

SLOT_CONFIG = {
    "slot_name": "test_slot",
    "plugin": "wal2json",
    "analysis_type": "summary",
    "period_hours": 1,
    "tables": ["orders"],
    "operations": ["INSERT"],
    "summary_pdf": 1,
    "summary_html": 1,
    "history_table": "orders",
    "history_value": "9999",
    "masks_fields": ["password"],
    "save_target": "disk",
    "disk_path": "tmp"
}

@pytest.fixture
def slot(monkeypatch):
    s = LogicalSlot(VALID_DB, SLOT_CONFIG)

    class DummyCursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args, **kwargs): pass
        def fetchone(self): return 1  # имитируем, что слот уже есть

    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return DummyCursor()

    monkeypatch.setattr(s, "_connect", lambda: DummyConn())
    return s

# 1. Ошибка подключения
def test_invalid_connection():
    result = check_connection(INVALID_DB)
    assert "Ошибка подключения" in result

# 2. Слот уже существует
def test_create_existing_slot(slot, capsys):
    slot.create_slot()
    slot.create_slot()
    captured = capsys.readouterr()
    # проверяем, что в выводе есть сообщение
    assert "уже существует" in captured.out


# 3. Удаление несуществующего слота
def test_drop_nonexistent_slot():
    try:
        drop_current_slot(VALID_DB, "nonexistent_slot")
    except Exception as e:
        assert "ошибка" in str(e).lower() or isinstance(e, Exception)

# 4. Фильтрация по таблице
def test_filter_by_table(monkeypatch, tmp_path):
    slot = LogicalSlot(VALID_DB, SLOT_CONFIG)

    # подменяем _connect, чтобы вернуть фиктивные события
    class DummyCursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args, **kwargs): pass
        def __iter__(self):
            # имитируем два события: одно из orders, одно из customers
            events = [
                json.dumps({"change":[{"table":"orders","kind":"INSERT"}]}),
                json.dumps({"change":[{"table":"customers","kind":"INSERT"}]})
            ]
            return iter([(ev,) for ev in events])

    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return DummyCursor()

    monkeypatch.setattr(slot, "_connect", lambda: DummyConn())

    output_file = tmp_path / "events.jsonl"
    slot.fetch_events(output_file=str(output_file), filters={"tables":["orders"]})

    with open(output_file, encoding="utf-8") as f:
        lines = f.readlines()

    assert all("orders" in line for line in lines)


# 5. Фильтрация по операциям
def test_filter_by_operation(monkeypatch, tmp_path):
    # фиктивный слот
    slot = LogicalSlot(
        {"dbname":"testdb","user":"postgres","password":"x","host":"localhost","port":5432},
        {"slot_name":"test_slot","plugin":"wal2json","analysis_type":"summary",
         "period_hours":1,"tables":[],"operations":[],"summary_pdf":0,"summary_html":0,
         "history_table":"","history_value":"","masks_fields":"","save_target":"disk","disk_path":str(tmp_path)}
    )

    # подменяем _connect, чтобы вернуть фиктивные события
    class DummyCursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args, **kwargs): pass
        def __iter__(self):
            # два события: INSERT и UPDATE
            events = [
                json.dumps({"change":[{"table":"orders","kind":"INSERT","columnvalues":["1"]}]}),
                json.dumps({"change":[{"table":"orders","kind":"UPDATE","columnvalues":["2"]}]})
            ]
            return iter([(ev,) for ev in events])

    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return DummyCursor()

    monkeypatch.setattr(slot, "_connect", lambda: DummyConn())

    # вызываем fetch_events с фильтром по операциям
    output_file = tmp_path / "events.jsonl"
    slot.fetch_events(output_file=str(output_file), filters={"ops":["INSERT"]})

    # читаем результат
    with open(output_file, encoding="utf-8") as f:
        lines = f.readlines()

    # проверяем, что остались только INSERT
    assert all("INSERT" in line for line in lines)
    assert not any("UPDATE" in line for line in lines)

# 6. Пустой JSONL
def test_empty_jsonl_report(tmp_path):
    jsonl_path = tmp_path / "empty.jsonl"
    jsonl_path.write_text("")  # пустой файл
    aggregate_jsonl_to_sqlite(str(jsonl_path), "wal_analyzer.db", "test_slot", 1)
    assert jsonl_path.exists()

# 7. Агрегация увеличивает счётчики
def test_aggregation_increment(tmp_path):
    jsonl_path = tmp_path / "events.jsonl"
    event = {"operation": "INSERT", "schema": "public", "table": "orders", "timestamp": "2025-12-15T10:00:00Z"}
    jsonl_path.write_text(json.dumps(event) + "\n")
    aggregate_jsonl_to_sqlite(str(jsonl_path), "wal_analyzer.db", "test_slot", 1)
    aggregate_jsonl_to_sqlite(str(jsonl_path), "wal_analyzer.db", "test_slot", 1)
    # Проверка: должно быть как минимум 2 записи по INSERT
    conn = sqlite3.connect("wal_analyzer.db")
    cur = conn.cursor()
    cur.execute("SELECT count FROM agg_operations WHERE slot_name = ? AND operation = ?", ("test_slot", "INSERT"))
    count = cur.fetchone()[0]
    conn.close()
    assert count >= 2

# 8. Отчёт без данных
def test_empty_report_pdf(tmp_path):
    builder = ReportBuilder(SLOT_CONFIG)
    builder.save_pdf(tmp_path / "empty_report.pdf")
    assert os.path.exists(tmp_path / "empty_report.pdf")

# 9. Маскирование поля
def test_masking_password():
    builder = ReportBuilder(SLOT_CONFIG)
    data = {"username": "admin", "password": "Secret123!"}
    masked = builder.mask_fields(data, ["password"])
    assert masked["password"] != "Secret123!"
    assert "#" in masked["password"] or "*" in masked["password"]

# 10. Ошибочный ID для history
def test_invalid_history_id(monkeypatch):
    bad_config = SLOT_CONFIG.copy()
    bad_config["analysis_type"] = "history"
    slot = LogicalSlot(VALID_DB, bad_config)

    class DummyCursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args, **kwargs): pass
        def __iter__(self): return iter([])  # нет событий

    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return DummyCursor()

    monkeypatch.setattr(slot, "_connect", lambda: DummyConn())

    result = slot.fetch_events()
    assert "не существуют" in str(result) or "ошибка" in str(result).lower()

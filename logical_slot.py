import psycopg2
from psycopg2 import sql
import json
from psycopg2 import OperationalError
from metabd import *
import sqlite3
from reportbuilder import ReportBuilder
import os
from datetime import datetime



def test_connection(db_config: dict) -> str:
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
    except OperationalError as e:
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


class LogicalSlot:
    def __init__(self, db_config, slot_config):
        print(db_config, slot_config)
        self.db_config = db_config
        self.slot_config = slot_config

        self.dbname = db_config.get('dbname')
        self.user = db_config.get('user')
        self.password = db_config.get('password')
        self.host = db_config.get('host', 'localhost')
        self.port = db_config.get('port', 5432)

        self.slot_name = slot_config.get('slot_name') or 'data_slot'
        self.plugin = slot_config.get('plugin') or 'wal2json'
        self.slot_config['plugin'] = slot_config.get('plugin') or 'wal2json'
        self.slot_config['slot_name'] = slot_config.get('slot_name') or 'data_slot'

        self.analysis_type = slot_config.get('analysis_type')

        if self.slot_config["history_value"]:
            self.ids = [v.strip() for v in self.slot_config["history_value"].split(";") if v.strip()]


        print(self.port, self.slot_name, self.plugin)
        
        if not all([self.dbname, self.user, self.password]):
            raise ValueError("Параметры dbname, user и password обязательны.")

    def _connect(self):
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        conn.autocommit = True
        return conn

    def slot_exists(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", (self.slot_name,))
                return cur.fetchone() is not None

    def create_slot(self):
        print("создание началось")
        if self.slot_exists():
            print(f"Слот '{self.slot_name}' уже существует.")
            return

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                print("Postgres version:", cur.fetchone())
                cur.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, %s);",
                    (self.slot_name, self.plugin)
                )
                print(f"Слот '{self.slot_name}' успешно создан с декодером '{self.plugin}'.")

        
    def fetch_events(self, output_file="events.jsonl", filters: dict = None):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT data FROM pg_logical_slot_get_changes(
                        %s, NULL, NULL,
                        'include-timestamp', '1',
                        'include-xids', '1',
                        'include-schemas', '1',
                        'include-types', '1',
                        'include-transaction', '1'
                    );
                """, (self.slot_name,))

                with open(output_file, "a", encoding="utf-8") as f:
                    for row in cur:
                        try:
                            change = json.loads(row[0])
                            for tx in change.get('change', []):
                                # --- фильтрация ---
                                if filters:
                                    tables = filters.get("tables") or []   # если пусто → все таблицы
                                    ops = filters.get("ops") or []         # если пусто → все операции
                                    ids = filters.get("ids") or []         # если пусто → все Id

                                    # фильтр по таблице
                                    if tables and tx.get("table") not in tables:
                                        continue
                                    # фильтр по операции
                                    if ops and tx.get("kind").upper() not in [op.upper() for op in ops]:
                                        continue
                                    # фильтр по Id (ищем в old_data/new_data)
                                    if ids:
                                        old_data = tx.get('oldkeys', {}).get('keyvalues') or {}
                                        new_data = tx.get('columnvalues') or {}
                                        # проверяем, встречается ли хотя бы один Id
                                        if not any(str(id_) in json.dumps(old_data) or str(id_) in json.dumps(new_data) for id_ in ids):
                                            continue

                                # --- событие ---
                                event = {
                                    'timestamp': change.get('timestamp'),
                                    'xid': change.get('xid'),
                                    'schema': tx.get('schema'),
                                    'table': tx.get('table'),
                                    'operation': tx.get('kind'),
                                    'old_data': tx.get('oldkeys', {}).get('keyvalues'),
                                    'new_data': tx.get('columnvalues')
                                }
                                f.write(json.dumps(event, ensure_ascii=False) + "\n")
                        except Exception as e:
                            print(f"Ошибка при разборе события: {e}")

        # если режим summary — сразу агрегируем
        if self.analysis_type == "summary":
            aggregate_jsonl_to_sqlite(
                jsonl_path="events.jsonl",
                sqlite_path="wal_analyzer.db",
                slot_name=self.slot_name,
                period_hours=self.slot_config['period_hours']
            )
            return 1
        if self.analysis_type == "history":
            try:
                builder = ReportBuilder(self.slot_config)
                result = builder.aggregate_jsonl_to_pdfs(
                    "events.jsonl",
                    self.slot_name,
                    self.slot_config["history_table"],
                    self.ids,
                    os.getcwd()
                )
                return f"reports pdf in {result}"
            except Exception as e:
                print(f"Ошибка в блоке history: {e}")
            
        return 0


    def drop_slot(self, result: str):
        drop_current_slot(self.db_config, self.slot_name)
        clear_sql(result, self.slot_name)

    def get_summary(self):
        try:
            builder = ReportBuilder(self.slot_config)
            builder.pie_operations()
            builder.activity_line()
            builder.heatmap_tables()
            builder.size_histogram()

            path = self.slot_config.get("disk_path") or os.getcwd()
            result = ""

            if self.slot_config.get('summary_pdf'):
                pdf_name = f"orders_report_{self.slot_config['slot_name']}.pdf"
                path_pdf = os.path.join(path, pdf_name)
                builder.save_pdf(path_pdf)
                result += path_pdf + ";"

            if self.slot_config.get('summary_html'):
                html_name = f"orders_report_{self.slot_config['slot_name']}.html"
                path_html = os.path.join(path, html_name)
                builder.save_html(path_html)
                result += path_html + ";"
                
            result = result if result else "Не выбрано расширение для отчета"
            return result
        except Exception as e:
            print(f"Ошибка в блоке summary: {e}")
    
    def fetch_events_full_save(self):

        # формируем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # расширение зависит от плагина
        if self.plugin == "wal2json":
            ext = "jsonl"
        elif self.plugin == "test_decoding":
            ext = "txt"
        else:
            raise ValueError(f"Неизвестный плагин: {self.plugin}")
        
        filename = f"{self.slot_name}_{timestamp}.{ext}"
        output_file = os.path.join(self.slot_config["disk_path"], filename)

        # фильтры из конфигурации
        filters = {
            "tables": self.slot_config.get("tables") or [],
            "ops": self.slot_config.get("operations") or ["INSERT","UPDATE","DELETE"]
        }

        # выбор плагина
        if self.plugin == "wal2json":
            result = self.fetch_events(output_file=output_file, filters=filters)
        elif self.plugin == "test_decoding":
            result = self.fetch_test_decoding(output_file=output_file, filters=filters)

        if result == 1:
            result = f"files .{ext} in {self.slot_config['disk_path']}"
            return result

    def fetch_test_decoding(self, output_file: str, filters: dict = None):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL);
                """, (self.slot_name,))

                with open(output_file, "a", encoding="utf-8") as f:
                    for row in cur:
                        line = row[0]

                        # простая фильтрация по таблицам/операциям
                        if filters:
                            tables = filters.get("tables") or []   # если пусто → все таблицы
                            ops = filters.get("ops") or []         # если пусто → все операции

                            if tables and not any(t in line for t in tables):
                                continue
                            if ops and not any(op.lower() in line.lower() for op in ops):
                                continue

                        f.write(line + "\n")

        return 1
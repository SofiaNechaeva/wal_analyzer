import psycopg2
from psycopg2 import sql
import json
from psycopg2 import OperationalError
from metabd import *
import sqlite3
from reportbuilder import ReportBuilder
import os
from datetime import datetime
import traceback




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

        if self.slot_config.get("masks_fields"):
            if isinstance(self.slot_config["masks_fields"], str):
                self.masks_fields = [f.strip() for f in self.slot_config["masks_fields"].split(";") if f.strip()]
            elif isinstance(self.slot_config["masks_fields"], list):
                self.masks_fields = [f.strip() for f in self.slot_config["masks_fields"] if f.strip()]
            else:
                self.masks_fields = []

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

                wrote_any = False
                with open(output_file, "a", encoding="utf-8") as f:
                    for row in cur:
                        wrote_any = True
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
                    # если не было ни одной строки — создаём пустую метку
                    if not wrote_any:
                        f.write("")  

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
                columns = get_table_columns(self.db_config, self.slot_config["history_table"])
                result = builder.aggregate_jsonl_to_pdfs(
                    "events.jsonl",
                    self.slot_name,
                    self.slot_config["history_table"],
                    self.ids,
                    os.getcwd(),
                    columns,
                    self.masks_fields
                    
                )
                return f"reports pdf in {result}"
            except Exception as e:
                print(f"Ошибка в блоке history: {e}")
            return "Такие первичные ключи не существуют или др. ошибка ввода"
        return 1


    def drop_slot(self, result: str):
        drop_current_slot(self.db_config, self.slot_name)
        clear_sql(result, self.slot_name, self.analysis_type)

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
            traceback.print_exc()
    
    def fetch_events_full_save(self):
        # фильтры из конфигурации
        filters = {
                "tables": self.slot_config.get("tables") or [],
                "ops": self.slot_config.get("operations") or ["INSERT","UPDATE","DELETE"]
            }
        if self.slot_config["save_target"] == "disk":
        # проверка пути
            disk_path = self.slot_config["disk_path"]
            if not os.path.isdir(disk_path):
                return "Такой путь не существует"

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

            # выбор плагина
            if self.plugin == "wal2json":
                result = self.fetch_events(output_file=output_file, filters=filters)
            elif self.plugin == "test_decoding":
                result = self.fetch_test_decoding(output_file=output_file, filters=filters)
            print("в правде ", result)
            if result == 1:
                result = f"files .{ext} in {self.slot_config['disk_path']}"
        else:
            result = save_wal_changes_to_log(self.db_config, self.slot_name, filters)
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
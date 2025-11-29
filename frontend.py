import datetime
import json
import psycopg2
from tkinter import *
from tkinter import ttk
from logical_slot import LogicalSlot
from controller import main_function, create_slot, worker_fetch_loop, worker_stop_correct, run_analysis_core
from logical_slot import test_connection
from logical_slot import get_tables
from metabd import init_sqlite, load_connections_data, save_connection
import signal, sys
import traceback
import random
import threading
import time
import queue
import psycopg2
import gc
import sqlite3

def handle_exit(sig, frame):
    print("Завершение программы...")
    sys.exit(0)

# ловим Ctrl+C
signal.signal(signal.SIGINT, handle_exit)

class WalAnalyzerApp:
    
    def __init__(self, root):
        self.root = root
        self.result_queue = queue.Queue()
        self.db_config = {} 
        self.root.title("Анализатор WAL")
        def on_close():
            print("Закрытие приложения...")
            for obj in gc.get_objects():
                if isinstance(obj, sqlite3.Connection):
                    print("Открытое соединение SQLite:", obj)

            for obj in gc.get_objects():
                if isinstance(obj, psycopg2.extensions.connection):
                    if not obj.closed:  # 0 = открыто, 1 = закрыто
                        print("Открытое соединение PostgreSQL:", obj.dsn)
            root.quit()
            root.destroy()

        root.protocol("WM_DELETE_WINDOW", on_close)
# --- запуск ---

        try:
            self.root.attributes("-zoomed", True)   # Linux
        except TclError:
            self.root.state("zoomed")               # Windows, Mac 

        # --- вкладки ---
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(expand=True, fill=BOTH)

        self.frame_ans = ttk.Frame(self.notebook)
        self.frame_bd = ttk.Frame(self.notebook)
        self.frame_slot = ttk.Frame(self.notebook)

        self.notebook.add(self.frame_bd, text="Параметры подключения")
        self.notebook.add(self.frame_ans, text="Подключения")
        self.notebook.tab(self.frame_ans, state="disabled")
        self.notebook.add(self.frame_slot, text="Создать анализ")
        self.notebook.tab(self.frame_slot, state="disabled")

        # инициализация вкладок
        self.init_bd_tab()
        self.init_slot_tab()
        self.init_ans_tab()

        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)

    def on_tab_changed(self, event):
        tab = event.widget.tab(event.widget.select(), "text")
        if tab == "Подключения":
            print('Нет, ну я пробывал')
            self.load_connections()

    # --- генерация имени подключения ---
    def generate_conn_name(self) -> str:
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        return f"slot_{timestamp}"

    # --- вкладка "Параметры подключения" ---
    def init_bd_tab(self):
        center_frame = ttk.Frame(self.frame_bd)
        center_frame.place(relx=0.5, rely=0.5, anchor="center")

        labels = ["Имя БД:", "Пользователь:", "Пароль:", "Хост:", "Порт:"]
        defaults = ["mydb", "postgres", "postgres", "localhost", "5433"]

        self.entries = {}
        for i, (label_text, default) in enumerate(zip(labels, defaults)):
            lbl = ttk.Label(center_frame, text=label_text)
            lbl.grid(row=i, column=0, padx=8, pady=6, sticky=W)

            ent = ttk.Entry(center_frame, width=25)
            ent.insert(0, default)
            ent.grid(row=i, column=1, padx=8, pady=6, sticky=EW)

            self.entries[label_text] = ent

        center_frame.columnconfigure(1, weight=1)

        connect_btn = ttk.Button(center_frame, text="Подключиться", command=self.connect_with_pg)
        connect_btn.grid(row=len(labels), column=0, columnspan=2, pady=12)

        self.msg_var = StringVar()
        msg_label = ttk.Label(center_frame, textvariable=self.msg_var, foreground="green")
        msg_label.grid(row=len(labels)+1, column=0, columnspan=2)

    def connect_with_pg(self):
        self.db_config = {
            "dbname": self.entries["Имя БД:"].get(),
            "user": self.entries["Пользователь:"].get(),
            "password": self.entries["Пароль:"].get(),
            "host": self.entries["Хост:"].get(),
            "port": self.entries["Порт:"].get(),
        }
        result = test_connection(self.db_config)
        self.msg_var.set(result)
        if "успешно" in result:
            self.load_tables()
            self.notebook.tab(self.frame_ans, state="normal")
            if "превышено" not in result:
            # разблокируем вкладку "Создать анализ"
                self.notebook.tab(self.frame_slot, state="normal")
            
                    # --- загрузка из SQLite ---
            self.load_connections()
            
    
    def load_tables(self):
        tables = get_tables(self.db_config)

        # убираем служебную таблицу из списка
        tables = [t for t in tables if t != "data_change_log"]

        # заполняем Listbox
        self.tables_list.delete(0, END)
        for t in tables:
            self.tables_list.insert(END, t)

        # заполняем Combobox для истории
        self.history_table_choice['values'] = tables
        if tables:
            self.history_table_choice.current(0)

    # --- вкладка "Подключения" ---
    def init_ans_tab(self):
        pw = PanedWindow(self.frame_ans, orient=VERTICAL)
        pw.pack(expand=True, fill=BOTH)

        lf_conn = ttk.LabelFrame(pw, text="Работающие подключения")
        pw.add(lf_conn)
        self.tree_conn = ttk.Treeview(lf_conn, columns=("name","type","date","plugin","db","active"), show="headings")
        self.tree_conn.pack(expand=True, fill=BOTH)
        self.tree_conn.tag_configure("error_deleted", background="#ffe6e6", foreground="#990000")
       
        for col in ("name","type","date","plugin","db"):
            self.tree_conn.heading(col, text=col)
            self.tree_conn.column(col, width=100, anchor="center")

        lf_res = ttk.LabelFrame(pw, text="Результаты готовых анализов")
        pw.add(lf_res)
        self.tree_res = ttk.Treeview(lf_res, columns=("name","type","date","plugin","db","result"), show="headings")
        self.tree_res.pack(expand=True, fill=BOTH)
       
        for col in ("name","type","date","plugin","db","result"):
            self.tree_res.heading(col, text=col)
            self.tree_res.column(col, width=100, anchor="center")

    # --- вкладка "Создать анализ" ---
    def init_slot_tab(self):
        main_frame = ttk.Frame(self.frame_slot)
        main_frame.pack(expand=True, fill=BOTH)

        # левая часть
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=0, column=0, sticky="nswe", padx=10, pady=10)

        # правая часть
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=0, column=1, sticky="nswe", padx=10, pady=10)

        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=2)
        main_frame.rowconfigure(0, weight=1)

        # --- ЛЕВАЯ ЧАСТЬ ---
        ttk.Label(left_frame, text="Выбрать таблицы:").grid(row=0, column=0, sticky=W, pady=5)

        tables_frame = ttk.Frame(left_frame)
        tables_frame.grid(row=1, column=0, sticky="we", pady=5)

        # exportselection=False — чтобы выбранное не сбрасывалось при переключении фокуса между списками
        self.tables_list = Listbox(tables_frame, selectmode=MULTIPLE, height=6, exportselection=False)
        scroll_tables = ttk.Scrollbar(tables_frame, orient=VERTICAL, command=self.tables_list.yview)
        self.tables_list.configure(yscrollcommand=scroll_tables.set)

        self.tables_list.pack(side=LEFT, fill=BOTH, expand=True)
        scroll_tables.pack(side=RIGHT, fill=Y)

        ttk.Label(left_frame, text="Период (часов):").grid(row=2, column=0, sticky=W, pady=5)
        self.period_spin = Spinbox(left_frame, from_=1, to=168, increment=1)
        self.period_spin.grid(row=3, column=0, sticky="we", pady=5)

        ttk.Label(left_frame, text="Типы операций:").grid(row=4, column=0, sticky=W, pady=5)
        # exportselection=False — сохраняет подсветку выбранного
        self.ops_list = Listbox(left_frame, selectmode=MULTIPLE, height=4, exportselection=False)
        for op in ["INSERT", "UPDATE", "DELETE"]:
            self.ops_list.insert(END, op)
        self.ops_list.grid(row=5, column=0, sticky="we", pady=5)

        ttk.Label(left_frame, text="Название подключения:").grid(row=6, column=0, sticky=W, pady=5)
        self.conn_name_entry = ttk.Entry(left_frame)
        self.conn_name_entry.insert(0, self.generate_conn_name())
        self.conn_name_entry.grid(row=7, column=0, sticky="we", pady=5)

        left_frame.rowconfigure(8, weight=1)

        self.run_btn = ttk.Button(left_frame, text="Запустить анализ", command=self.run_analysis)
        self.run_btn.grid(row=9, column=0, sticky="we", pady=15, ipady=10)

        left_frame.columnconfigure(0, weight=1)

        # --- ПРАВАЯ ЧАСТЬ ---
        ttk.Label(right_frame, text="Выберите тип анализа:").grid(row=0, column=0, sticky=W, pady=5)

        self.analysis_type = StringVar(value="summary")

        ttk.Radiobutton(right_frame, text="Сводка",
                        variable=self.analysis_type, value="summary").grid(row=1, column=0, sticky=W, pady=3)
        ttk.Radiobutton(right_frame, text="История изменений",
                        variable=self.analysis_type, value="history").grid(row=2, column=0, sticky=W, pady=3)
        ttk.Radiobutton(right_frame, text="Полные изменения",
                        variable=self.analysis_type, value="full").grid(row=3, column=0, sticky=W, pady=3)

        # блоки параметров
        self.frame_summary = ttk.LabelFrame(right_frame, text="Параметры сводки")
        self.frame_summary.grid(row=4, column=0, sticky="we", pady=20)
        self.pdf_var = IntVar()
        self.html_var = IntVar()
        ttk.Checkbutton(self.frame_summary, text="PDF", variable=self.pdf_var).grid(row=0, column=0, sticky=W)
        ttk.Checkbutton(self.frame_summary, text="HTML", variable=self.html_var).grid(row=0, column=1, sticky=W)

        self.frame_history = ttk.LabelFrame(right_frame, text="Параметры истории")
        self.frame_history.grid(row=5, column=0, sticky="we", pady=15)

        ttk.Label(self.frame_history, text="Таблица для истории:").grid(row=0, column=0, sticky=W, padx=5, pady=5)

        # Combobox вместо Listbox
        self.history_table_choice = ttk.Combobox(self.frame_history, state="readonly")
        self.history_table_choice.grid(row=0, column=1, sticky="we", padx=5, pady=5)

        ttk.Label(self.frame_history, text="Значения ключей через ;").grid(row=1, column=0, sticky=W, padx=5, pady=5)
        self.history_value_entry = ttk.Entry(self.frame_history)
        self.history_value_entry.grid(row=1, column=1, sticky="we", padx=5, pady=5)
        
        # Новое поле для маскирования
        ttk.Label(self.frame_history, text="Поля для маскирования через ;").grid(row=2, column=0, sticky=W, padx=5, pady=5)
        self.history_mask_entry = ttk.Entry(self.frame_history)
        self.history_mask_entry.grid(row=2, column=1, sticky="we", padx=5, pady=5)


        self.frame_history.columnconfigure(1, weight=1)

        self.frame_full = ttk.LabelFrame(right_frame, text="Параметры полных изменений")
        self.frame_full.grid(row=6, column=0, sticky="we", pady=15)

        self.save_target = StringVar(value="postgres")

        ttk.Radiobutton(self.frame_full, text="В таблицу Postgres (data_change_log)",
                        variable=self.save_target, value="postgres").grid(row=0, column=0, sticky=W, pady=8)
        ttk.Radiobutton(self.frame_full, text="На диск",
                        variable=self.save_target, value="disk").grid(row=1, column=0, sticky=W, pady=8)

        self.plugin_choice = StringVar()
        ttk.Label(self.frame_full, text="Формат:").grid(row=2, column=0, sticky=W, pady=8)
        # второй плагин — test_decoding, pgoutput убираем
        self.format_combo = ttk.Combobox(self.frame_full, textvariable=self.plugin_choice,
                                        values=["wal2json", "test_decoding"], state="disabled")
        self.format_combo.grid(row=2, column=1, sticky="we", pady=8)

        ttk.Label(self.frame_full, text="Путь к файлу:").grid(row=3, column=0, sticky=W, pady=8)
        self.disk_entry = ttk.Entry(self.frame_full, state="disabled")
        self.disk_entry.grid(row=3, column=1, sticky="we", pady=8)

        self.status_label = Label(right_frame, text="", fg="green")
        self.status_label.grid(row=10, column=0, sticky="w", pady=120)

        self.frame_full.columnconfigure(1, weight=1)

        def update_full_block(*args):
            if self.save_target.get() == "disk":
                self.format_combo.configure(state="normal")
                self.disk_entry.configure(state="normal")
            else:
                self.format_combo.configure(state="disabled")
                self.disk_entry.configure(state="disabled")

        self.save_target.trace_add("write", update_full_block)
        update_full_block()

        right_frame.columnconfigure(0, weight=1)

        # --- функция переключения доступности блоков ---
        def update_blocks(*args):
            sel = self.analysis_type.get()
            for child in self.frame_summary.winfo_children():
                child.configure(state="disabled")
            for child in self.frame_history.winfo_children():
                child.configure(state="disabled")
            for child in self.frame_full.winfo_children():
                child.configure(state="disabled")

            if sel == "summary":
                for child in self.frame_summary.winfo_children():
                    child.configure(state="normal")
            elif sel == "history":
                for child in self.frame_history.winfo_children():
                    child.configure(state="normal")
            elif sel == "full":
                for child in self.frame_full.winfo_children():
                    child.configure(state="normal")

        self.analysis_type.trace_add("write", update_blocks)
        update_blocks()

            # --- синхронизация списка истории с левым списком таблиц ---
        def sync_history_tables():
            # собрать список таблиц из левого списка
            tables = [self.tables_list.get(i) for i in range(self.tables_list.size())]

            # заполнить combobox для истории
            self.history_table_choice['values'] = tables

            # если слева что-то выбрано, по умолчанию выбрать первую выбранную для истории
            left_selected = list(self.tables_list.curselection())
            if left_selected:
                self.history_table_choice.set(self.tables_list.get(left_selected[0]))
            elif tables:
                self.history_table_choice.set(tables[0])
            else:
                self.history_table_choice.set("")
                
        # вызов при старте и при изменении выбора слева
        sync_history_tables()

    def collect_analysis_params(self) -> dict:
        # выбранные таблицы
        tables = [self.tables_list.get(i) for i in self.tables_list.curselection()] or []
        # выбранные операции (безопасный дефолт)
        operations = [self.ops_list.get(i) for i in self.ops_list.curselection()] or ["INSERT", "UPDATE", "DELETE"]

        history_table = self.history_table_choice.get() or (tables[0] if tables else "")
        history_value = self.history_value_entry.get()

       

        # плагин: по умолчанию test_decoding
        plugin = (self.plugin_choice.get() or "").strip() or "wal2json"

        slot_config = {
            "tables": tables,                         # [] трактуем как "все таблицы" на уровне backend
            "period_hours": int(self.period_spin.get()),
            "operations": operations,
            "slot_name": self.conn_name_entry.get(),
            "analysis_type": self.analysis_type.get(),
            "summary_pdf": bool(self.pdf_var.get()),
            "summary_html": bool(self.html_var.get()),
            "history_table": history_table,
            "history_value": history_value,
            "masks_fields": self.history_mask_entry.get(),
            "save_target": self.save_target.get(),
            "plugin": plugin,                        # "wal2json" | "test_decoding"
            "disk_path": self.disk_entry.get(),      # может быть пустым, если сохранение в Postgres
        }

        return slot_config


    def load_connections(self):
        """Обновляет список подключений и анализов в Treeview."""
        for item in self.tree_conn.get_children():
            self.tree_conn.delete(item)
        for item in self.tree_res.get_children():
            self.tree_res.delete(item)


        rows = load_connections_data(self.db_config)

        for row in rows:
            if row["result"] == "active":
                self.tree_conn.insert("", "end",
                                    values=(row["slot_name"], row["analysis_type"], row["date"],
                                            row["plugin"], row["db"], "active"))
            elif row["result"] == "error_deleted":
                self.tree_conn.insert("", "end",
                                    values=(row["slot_name"], row["analysis_type"], row["date"],
                                            row["plugin"], row["db"], row["result"]),
                                    tags=("error_deleted",))
            else:  # ready
                self.tree_res.insert("", "end",
                                    values=(row["slot_name"], row["analysis_type"], row["date"],
                                            row["plugin"], row["db"], row["result"]))

    # def run_analysis(self):
    #     def random_color():
    #         # генерируем три компоненты от 0 до 255
    #         r = random.randint(0, 255)
    #         g = random.randint(0, 255)
    #         b = random.randint(0, 255)
    #         # переводим в hex-строку
    #         return f"#{r:02x}{g:02x}{b:02x}"
    #     self.slot_config = self.collect_analysis_params()
    #     try:
    #         save_connection(self.db_config, self.slot_config)
    #         print("Запись в SQLite прошла успешно")
    #     except Exception as e:
    #         self.status_label.config(text=f"Ошибка анализа: {e}")
    #         print("Ошибка SQLite:", e)

    #     try:
    #         main_function(self.slot_config['slot_name'])
    #         self.load_connections()
    #         color = random_color()
    #         self.status_label.config(text="Слот успешно создан!", fg=color)
    #     except Exception as e:
    #         # self.msg_var_slot.set(f"Ошибка анализа: {e}")
    #         self.status_label.config(text=f"Ошибка анализа: {e}")
    #         traceback.print_exc() 
    def check_queue(self):
        try:
            result = self.result_queue.get_nowait()
            worker_stop_correct(self.slot_config, self.analysis, result)
        except queue.Empty:
            print("Очередь пуста — поток ничего не положил")
            worker_stop_correct(self.slot_config, self.analysis, None)


    def run_analysis(self):
        def random_color():
            # генерируем три компоненты от 0 до 255
            r = random.randint(0, 255)
            g = random.randint(0, 255)
            b = random.randint(0, 255)
            # переводим в hex-строку
            return f"#{r:02x}{g:02x}{b:02x}"
        self.slot_config = self.collect_analysis_params()
        print(self.slot_config)

        try:
            save_connection(self.db_config, self.slot_config)
            # self.analysis = create_slot(self.slot_config['slot_name'])
            

            # worker_thread = threading.Thread(
            #     target=worker_fetch_loop,
            #     args=(self.result_queue, self.analysis, self.slot_config, self.slot_config["period_hours"], 30), 
            # )
            # worker_thread.start()
            # self.root.after(self.slot_config["period_hours"]*1000+1000, self.check_queue)

            self.load_connections()
            color = random_color()
            self.status_label.config(text="Слот успешно создан!", fg=color)

            run_analysis_core(self.db_config, self.slot_config, self.result_queue)


        except Exception as e:
            # self.msg_var_slot.set(f"Ошибка анализа: {e}")
            self.status_label.config(text=f"Ошибка анализа: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    init_sqlite()
    root = Tk()
    app = WalAnalyzerApp(root)
    root.mainloop()


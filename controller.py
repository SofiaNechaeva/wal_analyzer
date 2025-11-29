from logical_slot import LogicalSlot
import json
import sqlite3
import time
import threading
import traceback

db_config = {
    'dbname': 'mydb',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5433
}

slot_config = {
    "tables": ["orders", "customers", "products"],  
    "period_hours": 24,                             
    "operations": ["INSERT", "UPDATE", "DELETE"],    
    "slot_name": "slot_orders_analysis",             
    "analysis_type": "summary",                  
    "summary_pdf": True,                            
    "summary_html": False,                           
    "history_table": "orders_history",               
    "history_value": "status",
    "masks_fields": "name",                       
    "save_target": "local",                         
    "plugin": "wal2json",                            
    "disk_path": "C:\Dev\wal_analizer"  
}


def get_configs(slot_name: str):
    conn = sqlite3.connect("wal_analyzer.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT dbname, user, password, host, port,
               tables, period_hours, operations,
               slot_name, analysis_type,
               summary_pdf, summary_html,
               history_table, history_value, masks_fields,
               save_target, plugin, disk_path, result
        FROM connections
        WHERE slot_name = ?
    """, (slot_name,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise ValueError(f"Слот {slot_name} не найден в connections")

    db_config = {
        "dbname": row[0],
        "user": row[1],
        "password": row[2],
        "host": row[3],
        "port": row[4],
    }

    slot_config = {
        "tables": json.loads(row[5]) if row[5] else [],
        "period_hours": row[6],
        "operations": json.loads(row[7]) if row[7] else [],
        "slot_name": row[8],
        "analysis_type": row[9],
        "summary_pdf": bool(row[10]),
        "summary_html": bool(row[11]),
        "history_table": row[12],
        "history_value": row[13],
        "masks_fields": row[14],
        "save_target": row[15],
        "plugin": row[16],
        "disk_path": row[17],
        "result": row[18],
    }

    return db_config, slot_config

def main_function(slot_name):
    db_config, slot_config = get_configs(slot_name)
    analysys = LogicalSlot(db_config, slot_config)
    analysys.create_slot()
    result = None
    if slot_config['analysis_type'] == 'summary':
        events = analysys.fetch_events()
        result = analysys.get_summary()
        print(events)
    elif slot_config['analysis_type'] == 'full':
        result = analysys.fetch_events_full_save()

    elif slot_config['analysis_type'] == 'history':
        result = analysys.fetch_events()
        
    analysys.drop_slot(result)

def create_slot(slot_name):
    db_config, slot_config = get_configs(slot_name)
    analysys = LogicalSlot(db_config, slot_config)
    analysys.create_slot()
    return analysys

def worker_fetch_loop(result_queue, analysys, slot_config, duration_seconds, interval_seconds):
    print("Я работник, я работаю)")
    import matplotlib
    matplotlib.use("Agg")  # безголовой режим для графиков
    """Фоновый поток: повторяет fetch пока не истечёт duration_seconds"""
    result = None
    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        try:
            if slot_config['analysis_type'] == 'summary':
                analysys.fetch_events()
                result = "summary_ready"
            elif slot_config['analysis_type'] == 'full':
                result = analysys.fetch_events_full_save()
                
            elif slot_config['analysis_type'] == 'history':
                result = analysys.fetch_events()
        except Exception as e:
            print("Ошибка при fetch:", e)
            traceback.print_exc()
        
        print("в потоке ", result)

        time.sleep(interval_seconds)
    
    # по окончании работы — положить результат
    # try:
    #     result_queue.put(result)
    # except Exception as e:
    #     print("Ошибка при помещении в очередь:", e)

    # по окончании работы — завершение
    worker_stop_correct(slot_config, analysys, result)


def worker_stop_correct(slot_config, analysys, result):
    try:
        if slot_config['analysis_type'] == 'summary':
            result = analysys.get_summary()  # тут строим отчёт
        analysys.drop_slot(result)
        print("Слот закрыт, анализ завершён")
    except Exception as e:
        print("Ошибка при завершении:", e)


def run_analysis_core(db_config, slot_config, result_queue):
    analysys = create_slot(slot_config['slot_name'])

    worker_thread = threading.Thread(
        target=worker_fetch_loop,
        args=(result_queue, analysys, slot_config, slot_config["period_hours"], 30),
    )
    worker_thread.start()

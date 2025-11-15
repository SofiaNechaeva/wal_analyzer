from logical_slot import LogicalSlot
import json
import sqlite3

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
               history_table, history_value,
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
        "save_target": row[14],
        "plugin": row[15],
        "disk_path": row[16],
        "result": row[17],
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
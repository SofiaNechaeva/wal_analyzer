import psycopg2
import random
import string
import time
import sys

db_config = {
    'dbname': 'mydb',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5433
}

def random_string(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def run_random_ops(n_seconds: int):
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    tables = ["just_numbers", "just_texts"]
    operations = ["insert", "update", "delete"]

    start = time.time()
    while time.time() - start < n_seconds:
        table = random.choice(tables)
        op = random.choice(operations)

        if table == "just_numbers":
            rand_num = random.randint(1, 1000000)
            if op == "insert":
                cur.execute("INSERT INTO just_numbers (number) VALUES (%s)", (rand_num,))
            elif op == "update":
                cur.execute("UPDATE just_numbers SET number = %s WHERE id = (SELECT id FROM just_numbers ORDER BY random() LIMIT 1)", (rand_num,))
            elif op == "delete":
                cur.execute("DELETE FROM just_numbers WHERE id = (SELECT id FROM just_numbers ORDER BY random() LIMIT 1)")
        else:  # just_texts
            # случайный размер текста: маленький, средний, большой
            size = random.choice([10, 2000, 15000])
            rand_text = random_string(size)
            if op == "insert":
                cur.execute("INSERT INTO just_texts (text) VALUES (%s)", (rand_text,))
            elif op == "update":
                cur.execute("UPDATE just_texts SET text = %s WHERE id = (SELECT id FROM just_texts ORDER BY random() LIMIT 1)", (rand_text,))
            elif op == "delete":
                cur.execute("DELETE FROM just_texts WHERE id = (SELECT id FROM just_texts ORDER BY random() LIMIT 1)")
        print('.', end='')
        time.sleep(0.5)

    cur.close()
    conn.close()



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python random_ops.py <seconds>")
        sys.exit(1)

    n = int(sys.argv[1])
    run_random_ops(n)

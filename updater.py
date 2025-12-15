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

def random_string(size=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def init_data(cur):
    # создаём 5–10 случайных строк в just_texts
    for _ in range(random.randint(5, 10)):
        size = random.choice([10, 2000, 15000])  # разные размеры
        cur.execute("INSERT INTO just_texts (text) VALUES (%s)", (random_string(size),))

    # создаём 5–10 случайных чисел в just_numbers
    for _ in range(random.randint(5, 10)):
        cur.execute("INSERT INTO just_numbers (number) VALUES (%s)", (random.randint(1, 1000000),))

def run_updates(n_seconds: int):
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    # инициализация данных
    init_data(cur)

    start = time.time()
    while time.time() - start < n_seconds:
        table = random.choice(["just_texts", "just_numbers"])

        if table == "just_texts":
            size = random.choice([10, 2000, 15000])
            new_text = random_string(size)
            cur.execute("""
                UPDATE just_texts
                SET text = %s
                WHERE id = (SELECT id FROM just_texts ORDER BY random() LIMIT 1)
            """, (new_text,))
            print(f"UPDATE just_texts -> {new_text[:20]}...")

        else:
            new_number = random.randint(1, 1000000)
            cur.execute("""
                UPDATE just_numbers
                SET number = %s
                WHERE id = (SELECT id FROM just_numbers ORDER BY random() LIMIT 1)
            """, (new_number,))
            print(f"UPDATE just_numbers -> {new_number}")

        time.sleep(1)

    cur.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python updater.py <seconds>")
        sys.exit(1)

    n = int(sys.argv[1])
    run_updates(n)

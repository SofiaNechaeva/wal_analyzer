# Наше приложение для анализа WAL

Приложение подключается к PostgreSQL и использует плагин wal2json для получения изменений в базе.

---

## 📥 Установка приложения
Скачайте проект с GitHub:
```bash
git clone https://github.com/SofiaNechaeva/wal_analyzer.git
cd wal_analyzer

```

## 🖥️ Требования к среде и версиям

- **PostgreSQL:** 13–17 (проверено на 17.7)
- **wal2json:** актуальная версия
- **Операционные системы:**
  - 🐧 Linux
  - 🪟 Windows
- **Клиентские утилиты:** `psql`, `git`, `make`, `gcc` (для сборки wal2json)
- **Права доступа:** возможность редактировать конфигурацию PostgreSQL  
  (`postgresql.conf`, `pg_hba.conf`)

## 🔌 Установка и настройка wal2json

- 📦 Репозиторий проекта: [eulerto/wal2json](https://github.com/eulerto/wal2json)

### 🐧 Linux
На Linux (Ubuntu/Debian/RHEL и т. п.) установка выполняется нативно.  
Следуйте инструкции в [README проекта wal2json](https://github.com/eulerto/wal2json).  
Например для  PostgreSQL apt repository:

```bash
sudo apt-get install postgresql-server-dev-17
tar -zxf wal2json-wal2json_2_6.tar.gz
cd wal2json-wal2json_2_6
export PATH=/usr/lib/postgresql/17/bin:$PATH
make
make install
```
👉 После этого плагин будет доступен в каталоге расширений PostgreSQL.


### 🪟 Windows
Установка теоретически возможна по инструкции в [README проекта wal2json](https://github.com/eulerto/wal2json).  

Однако, на Windows собрать wal2json напрямую затруднительно. Рекомендуется использовать Docker Desktop и готовить контейнер с Postgres + wal2json.

Пример Dockerfile 

```Dockerfile
FROM postgres:17

# Установим пакеты для сборки wal2json
RUN apt-get update && \
    apt-get install -y git make gcc postgresql-server-dev-17

# Скачиваем и собираем свежий wal2json
RUN git clone --depth 1 https://github.com/eulerto/wal2json.git /opt/wal2json && \
    cd /opt/wal2json && \
    make && make install && \
    rm -rf /opt/wal2json

# Настройки PostgreSQL для логической репликации
RUN echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_replication_slots = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_connections = 100" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "shared_buffers = 256MB" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "work_mem = 16MB" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "maintenance_work_mem = 64MB" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_size = 1GB" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "min_wal_size = 80MB" >> /usr/share/postgresql/postgresql.conf.sample

# Открываем доступ
RUN echo "host replication all 0.0.0.0/0 trust" >> /usr/share/postgresql/pg_hba.conf.sample

EXPOSE 5432
```

Сборка и запуск

```bash
docker build -t postgres-wal2json .
docker run -d --name my_postgres_wal2json \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=mydb \
  -p 5433:5432 \
  postgres-wal2json
```

📤 Перенос готовой базы в Docker
Если у вас есть база на Windows, её можно перенести в контейнер:

```bash
pg_dump -U postgres -d mydb > mydb.sql
docker cp mydb.sql my_postgres_wal2json:/tmp/mydb.sql
docker exec -it my_postgres_wal2json psql -U postgres -d mydb -f /tmp/mydb.sql
```



## 🚀 Запуск приложения

### 1. Подготовка окружения

- Установите **Python 3.10+** (проверено на 3.11).
- Установите виртуальное окружение:

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
```
### 2. Зависимости

- В проекте есть файл requirements.txt. 
- Установите зависимости:

```bash
pip install -r requirements.txt
```

### 3. Настройка PostgreSQL

- Убедитесь, что Postgres настроен для логической репликации:

В postgresql.conf:

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```
В pg_hba.conf:

```conf
host replication all 0.0.0.0/0 trust
```

**Создание пользователя для репликации на Linux**
В приложении для подключения к БД используются имя пользователя и пароль,
по умолчанию - postgres / postgres, но у пользователя postgres на Linux отсутствует пароль,
подключение к БД из терминала происходит путём ввода команды **sudo -u postgres psql**,
для подключения к БД из приложения нужно создать отдельного пользователя
и выдать ему права на доступ к БД, в том числе на репликацию.
Делается это при помощи следующих команд:

CREATE ROLE test_user WITH PASSWORD <пароль>;
GRANT ALL PRIVILEGES ON TABLE my_table TO test_user;
ALTER ROLE test_user REPLICATION;

### 4. Запуск приложения
- В корне проекта выполните:

```bash
python main.py
```

После запуска откроется окно подключения:

Имя БД: mydb
Пользователь: postgres
Пароль: postgres
Хост: localhost
Порт: 5433
Нажмите кнопку «Подключиться». Внизу появится сообщение об успешном соединении.

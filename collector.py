import json
import logging
from logging.handlers import TimedRotatingFileHandler
import threading
import asyncio
import time
import signal
import sys
import gzip
import os
import re
import subprocess
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import tempfile
import pandas as pd
import pyodbc
import aioodbc
import fdb
import aiohttp
from flask import Flask, render_template_string, jsonify, g
from flask_caching import Cache
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# =============================================================================
# GRACEFUL SHUTDOWN
# =============================================================================
shutdown_event = asyncio.Event()
active_connections = []
connections_lock = asyncio.Lock()


async def register_connection(conn):
    """Регистрация соединения для graceful shutdown"""
    async with connections_lock:
        active_connections.append(conn)


async def unregister_connection(conn):
    """Удаление соединения из реестра"""
    async with connections_lock:
        if conn in active_connections:
            active_connections.remove(conn)


def graceful_shutdown(signum, frame):
    """Обработчик сигнала остановки"""
    logging.info("=" * 60)
    logging.info("Получен сигнал остановки. Завершение работы...")
    
    # Устанавливаем флаг остановки (async event можно установить из синхронного кода через loop)
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(shutdown_event.set)
    except RuntimeError:
        # Если event loop не запущен
        pass
    
    # Закрываем пул соединений SQLAlchemy
    logging.info("Закрытие пулов соединений SQLAlchemy...")
    try:
        connection_pool.dispose_all()
    except Exception as e:
        logging.error(f"Ошибка закрытия пулов: {e}")
    
    # Закрываем executor для синхронных операций
    logging.info("Закрытие executor...")
    try:
        sync_executor.shutdown(wait=False)
    except Exception as e:
        logging.error(f"Ошибка закрытия executor: {e}")
    
    logging.info("Завершение работы...")
    logging.info("=" * 60)


async def close_connection_safe(conn):
    """Безопасное закрытие async соединения"""
    if conn:
        try:
            await unregister_connection(conn)
            await conn.close()
        except Exception as e:
            logging.debug(f"Ошибка закрытия соединения: {e}")


# Регистрация обработчиков сигналов
signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)


# =============================================================================
# ЗАГРУЗКА И ВАЛИДАЦИЯ КОНФИГУРАЦИИ
# =============================================================================
REQUIRED_CONFIG_KEYS = {
    'database': ['server', 'database', 'username', 'password'],
    'telegram': ['chat_id', 'bot_token'],
    'web': ['host', 'port'],
}

REQUIRED_MSSQL_SYNC_KEYS = ['source_server', 'source_db', 'source_table', 'source_user', 'source_pass', 'target_table']
REQUIRED_FIREBIRD_SYNC_KEYS = ['host', 'port', 'database', 'table', 'user', 'password', 'target_table', 'objid']
REQUIRED_TC2_KEYS = ['files_directory', 'target_table']


def validate_config(config):
    """Валидация конфигурации при запуске"""
    errors = []
    warnings = []
    
    # Проверка обязательных секций
    for section, keys in REQUIRED_CONFIG_KEYS.items():
        if section not in config:
            errors.append(f"Отсутствует секция '{section}'")
        else:
            for key in keys:
                if key not in config[section]:
                    errors.append(f"Отсутствует ключ '{section}.{key}'")
    
    # Проверка sync_mssql
    if 'sync_mssql' in config:
        for i, sync in enumerate(config['sync_mssql']):
            for key in REQUIRED_MSSQL_SYNC_KEYS:
                if key not in sync:
                    errors.append(f"sync_mssql[{i}]: отсутствует ключ '{key}'")
    
    # Проверка sync_firebird
    if 'sync_firebird' in config:
        for i, sync in enumerate(config['sync_firebird']):
            for key in REQUIRED_FIREBIRD_SYNC_KEYS:
                if key not in sync:
                    errors.append(f"sync_firebird[{i}]: отсутствует ключ '{key}'")
    
    # Проверка tc2_processor (опциональная секция)
    if 'tc2_processor' in config:
        tc2 = config['tc2_processor']
        if tc2.get('enabled', False):
            for key in REQUIRED_TC2_KEYS:
                if key not in tc2:
                    errors.append(f"tc2_processor: отсутствует ключ '{key}'")
    
    # Проверка типов
    if 'web' in config:
        if not isinstance(config['web'].get('port'), int):
            errors.append("web.port должен быть числом")
    
    if 'sync_interval' in config:
        if not isinstance(config['sync_interval'], (int, float)) or config['sync_interval'] <= 0:
            errors.append("sync_interval должен быть положительным числом")
    
    if 'notification_timeout' in config:
        if not isinstance(config['notification_timeout'], (int, float)) or config['notification_timeout'] <= 0:
            errors.append("notification_timeout должен быть положительным числом")
    
    return errors


def load_config(config_path='config.json'):
    """Загрузка и валидация конфигурации из JSON файла"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"ОШИБКА: Файл конфигурации {config_path} не найден!")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ОШИБКА: Некорректный JSON в {config_path}: {e}")
        sys.exit(1)
    
    # Валидация
    errors = validate_config(config)
    if errors:
        print("ОШИБКА: Некорректная конфигурация:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    
    return config


CONFIG = load_config()


# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ (ротация 7 дней + сжатие)
# =============================================================================
def namer(name):
    """Переименование ротированных логов с добавлением .gz"""
    return name + ".gz"


def rotator(source, dest):
    """Сжатие ротированного лога в gzip"""
    with open(source, 'rb') as f_in:
        with gzip.open(dest, 'wb') as f_out:
            f_out.writelines(f_in)
    os.remove(source)


log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = TimedRotatingFileHandler(
    "sync.log",
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8"
)
file_handler.setFormatter(log_formatter)
file_handler.suffix = "%Y-%m-%d"
file_handler.namer = namer
file_handler.rotator = rotator

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

# Отключаем лишние логи Flask/Werkzeug
logging.getLogger('werkzeug').setLevel(logging.WARNING)


# =============================================================================
# RETRY С ЭКСПОНЕНЦИАЛЬНЫМ BACKOFF (ASYNC)
# =============================================================================
async def retry_with_backoff_async(func, max_retries=5, base_delay=1, max_delay=300, exceptions=(Exception,)):
    """
    Асинхронная функция для повтора с экспоненциальным backoff
    
    Args:
        func: async функция для выполнения
        max_retries: максимальное количество попыток (0 = бесконечно)
        base_delay: начальная задержка в секундах
        max_delay: максимальная задержка в секундах
        exceptions: кортеж исключений для перехвата
    """
    retries = 0
    delay = base_delay
    
    while True:
        try:
            return await func()
        except exceptions as e:
            retries += 1
            
            if max_retries > 0 and retries >= max_retries:
                logging.error(f"Превышено количество попыток ({max_retries}): {e}")
                raise
            
            # Экспоненциальный backoff с jitter
            jitter = delay * 0.1 * (0.5 - time.time() % 1)  # ±10% jitter
            actual_delay = min(delay + jitter, max_delay)
            
            logging.warning(f"Попытка {retries}, повтор через {actual_delay:.1f} сек: {e}")
            
            # Проверяем shutdown перед ожиданием
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=actual_delay)
                raise KeyboardInterrupt("Shutdown requested")
            except asyncio.TimeoutError:
                pass
            
            # Увеличиваем задержку экспоненциально
            delay = min(delay * 2, max_delay)


# =============================================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# =============================================================================
# Кэш RECTIME для уменьшения запросов к БД
rectime_cache = {}
rectime_cache_lock = None  # Инициализируется в async_main

# Для отслеживания уведомлений
sent_notifications = {}
notifications_lock = None  # Инициализируется в async_main

# Статус задач для healthcheck (вместо потоков)
task_status = {}
task_status_lock = None  # Инициализируется в async_main

# Время запуска
START_TIME = datetime.now()

EXCLUDED_COLUMNS = {'ID', 'H1', 'H2', 'H3', 'H4', 'OBJID', 'ObjectId', 'P3', 'P4', 'RecordId', 'T4', 'T5', 'T6', 'T7', 'T8', 'V4', 'V5'}


# =============================================================================
# RATE LIMITING ДЛЯ TELEGRAM
# =============================================================================
class TelegramRateLimiter:
    """Rate limiter для защиты от спама уведомлениями в Telegram"""
    
    def __init__(self, max_messages=5, window_seconds=60, cooldown_seconds=300):
        """
        Args:
            max_messages: максимум сообщений за период
            window_seconds: период в секундах
            cooldown_seconds: cooldown после достижения лимита
        """
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_seconds
        self.message_times = deque()
        self.lock = None  # Будет инициализирован в async_main
        self.cooldown_until = None
        self.suppressed_count = 0
    
    async def can_send(self):
        """Проверка возможности отправки сообщения"""
        if not self.lock:
            # Fallback если lock не инициализирован
            return True
        async with self.lock:
            now = datetime.now()
            
            # Проверяем cooldown
            if self.cooldown_until and now < self.cooldown_until:
                self.suppressed_count += 1
                return False
            elif self.cooldown_until and now >= self.cooldown_until:
                # Cooldown закончился
                if self.suppressed_count > 0:
                    logging.info(f"Telegram rate limit: подавлено {self.suppressed_count} сообщений за cooldown")
                self.cooldown_until = None
                self.suppressed_count = 0
                self.message_times.clear()
            
            # Удаляем старые записи
            cutoff = now - timedelta(seconds=self.window_seconds)
            while self.message_times and self.message_times[0] < cutoff:
                self.message_times.popleft()
            
            # Проверяем лимит
            if len(self.message_times) >= self.max_messages:
                self.cooldown_until = now + timedelta(seconds=self.cooldown_seconds)
                self.suppressed_count = 1
                logging.warning(f"Telegram rate limit: достигнут лимит ({self.max_messages}/{self.window_seconds}s), cooldown {self.cooldown_seconds}s")
                return False
            
            return True
    
    async def record_sent(self):
        """Записать факт отправки сообщения"""
        if self.lock:
            async with self.lock:
                self.message_times.append(datetime.now())
        else:
            self.message_times.append(datetime.now())
    
    async def get_status(self):
        """Статус для healthcheck"""
        if not self.lock:
            return {}
        async with self.lock:
            now = datetime.now()
            cutoff = now - timedelta(seconds=self.window_seconds)
            while self.message_times and self.message_times[0] < cutoff:
                self.message_times.popleft()
            
            return {
                'messages_in_window': len(self.message_times),
                'max_messages': self.max_messages,
                'window_seconds': self.window_seconds,
                'in_cooldown': self.cooldown_until is not None and now < self.cooldown_until,
                'cooldown_remaining': (self.cooldown_until - now).total_seconds() if self.cooldown_until and now < self.cooldown_until else 0,
                'suppressed_count': self.suppressed_count
            }


# Инициализация rate limiter
telegram_rate_limiter = TelegramRateLimiter(
    max_messages=CONFIG.get('telegram', {}).get('rate_limit_messages', 5),
    window_seconds=CONFIG.get('telegram', {}).get('rate_limit_window', 60),
    cooldown_seconds=CONFIG.get('telegram', {}).get('rate_limit_cooldown', 300)
)


# =============================================================================
# ПУЛ СОЕДИНЕНИЙ SQLALCHEMY
# =============================================================================
class ConnectionPool:
    """Пул соединений для MSSQL через SQLAlchemy"""
    
    def __init__(self):
        self.engines = {}
        self.lock = threading.Lock()
    
    def get_engine(self, server, database, uid, pwd):
        """Получение или создание engine для сервера"""
        key = f"{server}|{database}|{uid}"
        
        with self.lock:
            if key not in self.engines:
                # Создаем connection string для SQLAlchemy + pyodbc
                connection_string = (
                    f"mssql+pyodbc://{uid}:{pwd}@{server}/{database}"
                    f"?driver=SQL+Server&TrustServerCertificate=yes"
                )
                
                engine = create_engine(
                    connection_string,
                    poolclass=QueuePool,
                    pool_size=5,           # Базовый размер пула
                    max_overflow=10,       # Дополнительные соединения при нагрузке
                    pool_timeout=30,       # Таймаут ожидания соединения
                    pool_recycle=3600,     # Пересоздание соединений каждый час
                    pool_pre_ping=True,    # Проверка соединения перед использованием
                    echo=False
                )
                
                self.engines[key] = engine
                logging.info(f"Создан пул соединений для {server}/{database}")
            
            return self.engines[key]
    
    def get_connection(self, server, database, uid, pwd):
        """Получение соединения из пула"""
        engine = self.get_engine(server, database, uid, pwd)
        return engine.connect()
    
    def get_raw_connection(self, server, database, uid, pwd):
        """Получение raw pyodbc connection из пула"""
        engine = self.get_engine(server, database, uid, pwd)
        return engine.raw_connection()
    
    def get_pool_status(self):
        """Статус пулов для healthcheck"""
        status = {}
        with self.lock:
            for key, engine in self.engines.items():
                pool = engine.pool
                status[key] = {
                    'size': pool.size(),
                    'checked_in': pool.checkedin(),
                    'checked_out': pool.checkedout(),
                    'overflow': pool.overflow(),
                    'invalid': pool.invalidated()
                }
        return status
    
    def dispose_all(self):
        """Закрытие всех пулов"""
        with self.lock:
            for key, engine in self.engines.items():
                try:
                    engine.dispose()
                    logging.info(f"Закрыт пул соединений {key}")
                except Exception as e:
                    logging.error(f"Ошибка закрытия пула {key}: {e}")
            self.engines.clear()


# Глобальный пул соединений
connection_pool = ConnectionPool()


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ FIREBIRD
# =============================================================================
os.environ["ISC_USER"] = "sysdba"
os.environ["ISC_PASSWORD"] = "masterkey"

try:
    fdb.load_api('./fbclient.dll')
except fdb.fbcore.DatabaseError as e:
    logging.error(f"Ошибка загрузки Firebird API: {e}")
    raise


# =============================================================================
# FLASK ПРИЛОЖЕНИЕ
# =============================================================================
app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300})


def get_web_db_connection():
    """Подключение к БД для веб-интерфейса"""
    if 'db' not in g:
        db_config = CONFIG['database']
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
        )
        g.db = pyodbc.connect(connection_string)
    return g.db


@app.teardown_appcontext
def close_db_connection(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


@app.route('/health')
def health():
    """Healthcheck эндпоинт для мониторинга (синхронный, для Flask)"""
    uptime = datetime.now() - START_TIME
    
    # Получаем статус задач (прямой доступ к dict, т.к. Flask синхронный)
    # В production лучше использовать asyncio для получения статуса
    tasks_info = dict(task_status) if task_status else {}
    
    total_tasks = len(tasks_info)
    healthy_tasks = sum(1 for t in tasks_info.values() if t.get('healthy', False))
    
    if total_tasks == 0:
        status = 'starting'
        http_status = 503
    elif healthy_tasks == total_tasks:
        status = 'healthy'
        http_status = 200
    elif healthy_tasks > 0:
        status = 'degraded'
        http_status = 200
    else:
        status = 'unhealthy'
        http_status = 503
    
    response = {
        'status': status,
        'uptime_seconds': int(uptime.total_seconds()),
        'uptime': str(uptime).split('.')[0],
        'tasks': {
            'total': total_tasks,
            'healthy': healthy_tasks,
            'details': tasks_info
        },
        'cache': {
            'rectime_entries': len(rectime_cache)
        },
        'connection_pool': connection_pool.get_pool_status() if connection_pool else {},
        'telegram_rate_limit': {},  # Будет заполняться при наличии async loop
        'shutdown_requested': shutdown_event.is_set(),
        'timestamp': datetime.now().isoformat()
    }
    
    return jsonify(response), http_status




def get_latest_data():
    conn = get_web_db_connection()
    cursor = conn.cursor()
    result_data = []
    all_columns = set()

    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE 'Dynamic_%'")
    tables = cursor.fetchall()

    table_names = CONFIG.get('table_names', {})

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        columns_available = {row[0] for row in cursor.fetchall()} - EXCLUDED_COLUMNS

        if 'RECTIME' not in columns_available:
            continue

        cursor.execute(f"SELECT TOP 1 {', '.join(columns_available)} FROM {table_name} ORDER BY RECTIME DESC")
        result = cursor.fetchone()

        if result:
            row = dict(zip(columns_available, result))
            rectime = row.get('RECTIME')

            if isinstance(rectime, str):
                rectime = datetime.strptime(rectime, '%Y-%m-%d %H:%M:%S.%f')

            row = {key: row.get(key, 0) for key in columns_available}
            outdated = datetime.now() - timedelta(hours=1) > rectime if rectime else False

            row['TABLE_NAME'] = table_names.get(table_name, table_name)
            row['RECTIME'] = rectime.strftime('%Y-%m-%d %H:%M:%S') if rectime else 'No Data'
            row['outdated'] = outdated

            result_data.append(row)
            all_columns.update(columns_available)
        else:
            row = {col: 'No Data' for col in columns_available}
            row['TABLE_NAME'] = table_names.get(table_name, table_name)
            row['RECTIME'] = 'No Data'
            row['outdated'] = True

            result_data.append(row)
            all_columns.update(columns_available)

        all_columns.add('TABLE_NAME')
        all_columns.add('RECTIME')

    return result_data, all_columns


@app.route('/data')
def data():
    """API endpoint для получения последних данных (без кэширования)"""
    data, _ = get_latest_data()
    response = jsonify(data)
    # Заголовки для предотвращения кэширования в браузере
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/')
def index():
    data, all_columns = get_latest_data()

    column_order = ['TABLE_NAME', 'RECTIME', 'T1', 'T2', 'T3', 'P1', 'P2', 'V1', 'V2', 'V3']
    ordered_columns = [col for col in column_order if col in all_columns] + \
                      sorted(all_columns - set(column_order))
    
    column_display_names = {
        'TABLE_NAME': 'Объект',
        'RECTIME': 'Время записи',
        'T1': 'T1 пр.факт',
        'T2': 'T2 обр.факт',
        'T3': 'T3 хол.факт',
        'P1': 'P1 пр.факт',
        'P2': 'P2 обр.факт',
        'V1': 'V1',
        'V2': 'V2'
    }

    template = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SCADA Collector - Мониторинг данных</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            :root {
                --bg-primary: #0f172a;
                --bg-secondary: #1e293b;
                --bg-card: #1e293b;
                --bg-hover: #334155;
                --text-primary: #f1f5f9;
                --text-secondary: #cbd5e1;
                --accent: #3b82f6;
                --accent-hover: #2563eb;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
                --border: #334155;
                --shadow: rgba(0, 0, 0, 0.3);
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                background: linear-gradient(135deg, var(--bg-primary) 0%, #1a2332 100%);
                color: var(--text-primary);
                min-height: 100vh;
                padding: 10px;
                line-height: 1.4;
            }
            
            .header {
                background: var(--bg-card);
                border-radius: 8px;
                padding: 10px 16px;
                margin-bottom: 10px;
                box-shadow: 0 4px 16px var(--shadow);
                display: flex;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                gap: 10px;
                border: 1px solid var(--border);
            }
            
            .header h1 {
                font-size: 18px;
                font-weight: 700;
                background: linear-gradient(135deg, var(--accent) 0%, #8b5cf6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                margin: 0;
            }
            
            .status-indicator {
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 11px;
            }
            
            .status-dot {
                width: 8px;
                height: 8px;
                border-radius: 50%;
                background: var(--success);
                animation: pulse 2s infinite;
                box-shadow: 0 0 6px var(--success);
            }
            
            .status-dot.updating {
                background: var(--warning);
                box-shadow: 0 0 6px var(--warning);
            }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }
            
            .last-update {
                color: var(--text-secondary);
                font-size: 10px;
            }
            
            .update-indicator {
                display: none;
                color: var(--accent);
                font-size: 10px;
                animation: blink 1s infinite;
            }
            
            .update-indicator.active {
                display: inline;
            }
            
            @keyframes blink {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.3; }
            }
            
            .container {
                background: var(--bg-card);
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 4px 16px var(--shadow);
                border: 1px solid var(--border);
                overflow: hidden;
                position: relative;
            }
            
            .table-wrapper {
                overflow-x: auto;
                border-radius: 6px;
                margin-top: 0;
            }
            
            table {
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                font-size: 12px;
                background: transparent;
                table-layout: fixed; /* Фиксированная ширина ячеек */
            }
            
            thead {
                position: sticky;
                top: 0;
                z-index: 10;
            }
            
            th {
                background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-hover) 100%);
                color: var(--text-primary);
                font-weight: 600;
                padding: 8px 10px;
                text-align: left;
                border-bottom: 2px solid var(--border);
                white-space: nowrap;
                cursor: pointer;
                transition: all 0.2s ease;
                user-select: none;
                position: relative;
                font-size: 11px;
                overflow: hidden;
                text-overflow: ellipsis;
            }
            
            /* Фиксированные ширины для основных колонок */
            th[data-column="TABLE_NAME"] { width: 200px; min-width: 200px; max-width: 200px; }
            th[data-column="RECTIME"] { width: 150px; min-width: 150px; max-width: 150px; }
            th[data-column="T1"], th[data-column="T2"], th[data-column="T3"] { width: 100px; min-width: 100px; max-width: 100px; }
            th[data-column="P1"], th[data-column="P2"] { width: 100px; min-width: 100px; max-width: 100px; }
            th[data-column="V1"], th[data-column="V2"], th[data-column="V3"] { width: 120px; min-width: 120px; max-width: 120px; }
            th[data-column="H1"], th[data-column="H2"], th[data-column="H3"], th[data-column="H4"] { width: 100px; min-width: 100px; max-width: 100px; }
            
            /* Для остальных колонок - фиксированная ширина по умолчанию */
            th:not([data-column="TABLE_NAME"]):not([data-column="RECTIME"]):not([data-column^="T"]):not([data-column^="P"]):not([data-column^="V"]):not([data-column^="H"]) {
                width: 100px;
                min-width: 100px;
                max-width: 100px;
            }
            
            th:hover {
                background: var(--bg-hover);
            }
            
            th.sortable::after {
                content: ' ↕';
                opacity: 0.5;
                font-size: 9px;
            }
            
            th.sort-asc::after {
                content: ' ↑';
                opacity: 1;
                color: var(--accent);
            }
            
            th.sort-desc::after {
                content: ' ↓';
                opacity: 1;
                color: var(--accent);
            }
            
            td {
                padding: 6px 10px;
                border-bottom: 1px solid var(--border);
                transition: all 0.2s ease;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
                width: auto; /* Ширина наследуется от th */
                min-width: 0; /* Позволяет ячейке сжиматься */
                box-sizing: border-box;
            }
            
            tbody tr {
                background: transparent;
                transition: all 0.2s ease;
            }
            
            tbody tr:hover {
                background: var(--bg-hover);
            }
            
            tbody tr.outdated {
                background: rgba(239, 68, 68, 0.1);
                border-left: 2px solid var(--danger);
            }
            
            tbody tr.outdated:hover {
                background: rgba(239, 68, 68, 0.2);
            }
            
            .data-cell.updated {
                animation: highlightUpdate 1.5s ease-out;
                display: inline-block;
                padding: 1px 3px;
                border-radius: 3px;
                position: relative;
            }
            
            @keyframes highlightUpdate {
                0% {
                    background-color: rgba(16, 185, 129, 0.5);
                    color: #ffffff;
                    box-shadow: 0 0 6px rgba(16, 185, 129, 0.4);
                    transform: scale(1.02);
                }
                50% {
                    background-color: rgba(16, 185, 129, 0.3);
                    box-shadow: 0 0 4px rgba(16, 185, 129, 0.3);
                }
                100% {
                    background-color: transparent;
                    color: inherit;
                    box-shadow: none;
                    transform: scale(1);
                }
            }
            
            .data-cell {
                font-weight: 400;
                display: inline-block;
                width: 100%;
                overflow: hidden;
                text-overflow: ellipsis;
            }
            
            .data-cell.number {
                font-family: 'Courier New', monospace;
                color: var(--accent);
                font-size: 11px;
                text-align: right;
            }
            
            .data-cell.time {
                color: var(--text-secondary);
                font-size: 10px;
            }
            
            /* Дополнительная фиксация для ячеек с числами */
            td:has(.data-cell.number) {
                text-align: right;
            }
            
            .badge {
                display: inline-block;
                padding: 4px 10px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: 600;
                text-transform: uppercase;
            }
            
            .badge-success {
                background: rgba(16, 185, 129, 0.2);
                color: var(--success);
            }
            
            .badge-danger {
                background: rgba(239, 68, 68, 0.2);
                color: var(--danger);
            }
            
            @keyframes fadeIn {
                from {
                    opacity: 0;
                    transform: translateY(10px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }
            
            tbody tr {
                animation: fadeIn 0.3s ease;
            }
            
            .loading-indicator {
                position: fixed;
                top: 10px;
                right: 10px;
                background: var(--bg-card);
                padding: 6px 12px;
                border-radius: 6px;
                font-size: 10px;
                color: var(--accent);
                box-shadow: 0 2px 8px var(--shadow);
                z-index: 1000;
                display: none;
                align-items: center;
                gap: 6px;
                border: 1px solid var(--border);
            }
            
            .loading-indicator.active {
                display: flex;
            }
            
            .mini-spinner {
                width: 12px;
                height: 12px;
                border: 2px solid var(--border);
                border-top-color: var(--accent);
                border-radius: 50%;
                animation: spin 0.6s linear infinite;
            }
            
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
            
            @media (max-width: 768px) {
                body {
                    padding: 8px;
                }
                
                .header {
                    padding: 8px 12px;
                }
                
                .header h1 {
                    font-size: 16px;
                }
                
                .container {
                    padding: 8px;
                }
                
                th, td {
                    padding: 6px 8px;
                    font-size: 11px;
                }
                
                table {
                    font-size: 11px;
                }
            }
        </style>
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script>
            let sortColumn = 'TABLE_NAME';
            let sortOrder = 'asc';
            let updateInterval = 5000; // 5 секунд
            let updateTimer = null;
            let isUpdating = false;
            let previousData = {}; // Хранение предыдущих данных для сравнения
            
            const orderedColumns = {{ ordered_columns|tojson }};
            const columnDisplayNames = {{ column_display_names|tojson }};
            
            function formatValue(value, column) {
                if (value === null || value === undefined || value === 'No Data') {
                    return '<span class="data-cell" style="color: var(--text-secondary);">—</span>';
                }
                
                if (column === 'RECTIME') {
                    return `<span class="data-cell time">${value}</span>`;
                }
                
                if (typeof value === 'number') {
                    return `<span class="data-cell number">${value.toFixed(2)}</span>`;
                }
                
                return `<span class="data-cell">${value}</span>`;
            }
            
            function normalizeValue(value) {
                if (value === null || value === undefined || value === 'No Data' || value === '' || value === '—') {
                    return null;
                }
                if (typeof value === 'number') {
                    return Math.round(value * 100) / 100; // Округление до 2 знаков
                }
                // Попытка распарсить число из строки
                const numValue = parseFloat(value);
                if (!isNaN(numValue) && isFinite(numValue)) {
                    return Math.round(numValue * 100) / 100;
                }
                return String(value).trim();
            }
            
            function updateCell(rowElement, columnIndex, newValue, column) {
                const cell = rowElement.find('td').eq(columnIndex);
                const dataCell = cell.find('.data-cell');
                const oldValue = dataCell.length > 0 ? dataCell.text().trim() : cell.text().trim();
                
                // Сохраняем текущую ширину ячейки перед обновлением
                const currentWidth = cell.width();
                
                // Нормализуем значения для сравнения
                const normalizedOld = normalizeValue(oldValue);
                const normalizedNew = normalizeValue(newValue);
                
                // Проверяем, изменилось ли значение
                if (normalizedOld !== normalizedNew) {
                    // Обновляем содержимое ячейки
                    cell.html(formatValue(newValue, column));
                    
                    // Восстанавливаем ширину ячейки, чтобы она не менялась
                    if (currentWidth > 0) {
                        cell.css('width', currentWidth + 'px');
                    }
                    
                    // Находим новый элемент со значением и добавляем класс подсветки
                    const newDataCell = cell.find('.data-cell');
                    if (newDataCell.length > 0) {
                        newDataCell.addClass('updated');
                        
                        // Удаляем класс подсветки через 1.5 секунды
                        setTimeout(() => {
                            newDataCell.removeClass('updated');
                        }, 1500);
                    }
                }
            }
            
            function updateRowStatus(rowElement, rowData) {
                const rowClass = rowData.outdated ? 'outdated' : '';
                if (rowData.outdated) {
                    rowElement.addClass('outdated');
                } else {
                    rowElement.removeClass('outdated');
                }
            }
            
            function loadData() {
                if (isUpdating) return;
                
                isUpdating = true;
                $('.status-dot').addClass('updating');
                $('.loading-indicator').addClass('active');
                $('.update-indicator').addClass('active');
                
                $.ajax({
                    url: '/data',
                    method: 'GET',
                    cache: false,
                    success: function(newData) {
                        // Сортировка
                        newData.sort((a, b) => {
                            let valA = a[sortColumn] || '';
                            let valB = b[sortColumn] || '';
                            
                            if (sortOrder === 'asc') {
                                return valA > valB ? 1 : valA < valB ? -1 : 0;
                            } else {
                                return valA < valB ? 1 : valA > valB ? -1 : 0;
                            }
                        });
                        
                        const tbody = $('#data-table tbody');
                        const isFirstLoad = Object.keys(previousData).length === 0;
                        
                        if (isFirstLoad) {
                            // Первая загрузка - генерируем всю таблицу
                            let tableBody = '';
                            newData.forEach(function(row) {
                                const rowClass = row.outdated ? 'outdated' : '';
                                tableBody += `<tr data-table-name="${row.TABLE_NAME || ''}" class="${rowClass}">`;
                                
                                orderedColumns.forEach(function(col) {
                                    const value = row[col] || '';
                                    tableBody += `<td>${formatValue(value, col)}</td>`;
                                });
                                
                                tableBody += '</tr>';
                            });
                            
                            tbody.html(tableBody);
                            
                            // Сохраняем данные для следующего сравнения
                            newData.forEach(function(row) {
                                if (row.TABLE_NAME) {
                                    previousData[row.TABLE_NAME] = {...row};
                                }
                            });
                        } else {
                            // Обновление существующих данных
                            newData.forEach(function(rowData) {
                                const tableName = rowData.TABLE_NAME;
                                if (!tableName) return;
                                
                                // Находим строку в таблице
                                let rowElement = tbody.find(`tr[data-table-name="${tableName}"]`);
                                
                                if (rowElement.length === 0) {
                                    // Новая строка - добавляем полностью
                                    const rowClass = rowData.outdated ? 'outdated' : '';
                                    let newRow = `<tr data-table-name="${tableName}" class="${rowClass}">`;
                                    orderedColumns.forEach(function(col) {
                                        const value = rowData[col] || '';
                                        newRow += `<td>${formatValue(value, col)}</td>`;
                                    });
                                    newRow += '</tr>';
                                    tbody.append(newRow);
                                    previousData[tableName] = {...rowData};
                                    return;
                                }
                                
                                // Обновляем статус строки (outdated)
                                updateRowStatus(rowElement, rowData);
                                
                                // Обновляем каждую ячейку
                                const prevRow = previousData[tableName];
                                orderedColumns.forEach(function(col, colIndex) {
                                    const newValue = rowData[col];
                                    const oldValue = prevRow ? prevRow[col] : null;
                                    
                                    // Сравниваем значения
                                    const normalizedOld = normalizeValue(oldValue);
                                    const normalizedNew = normalizeValue(newValue);
                                    
                                    if (normalizedOld !== normalizedNew) {
                                        updateCell(rowElement, colIndex, newValue, col);
                                    }
                                });
                                
                                // Обновляем сохраненные данные
                                previousData[tableName] = {...rowData};
                            });
                            
                            // Удаляем строки, которых больше нет в новых данных
                            const newTableNames = new Set(newData.map(r => r.TABLE_NAME).filter(Boolean));
                            tbody.find('tr').each(function() {
                                const tableName = $(this).data('table-name');
                                if (tableName && !newTableNames.has(tableName)) {
                                    $(this).remove();
                                    delete previousData[tableName];
                                }
                            });
                        }
                        
                        // Обновление времени последнего обновления
                        const now = new Date();
                        $('.last-update').text(`Обновлено: ${now.toLocaleTimeString('ru-RU')}`);
                        
                        // Обновление заголовков сортировки
                        $('th').removeClass('sort-asc sort-desc');
                        $(`th[data-column="${sortColumn}"]`).addClass(`sort-${sortOrder}`);
                    },
                    error: function(xhr, status, error) {
                        console.error('Ошибка загрузки данных:', error);
                    },
                    complete: function() {
                        isUpdating = false;
                        $('.status-dot').removeClass('updating');
                        $('.loading-indicator').removeClass('active');
                        setTimeout(() => {
                            $('.update-indicator').removeClass('active');
                        }, 500);
                    }
                });
            }
            
            function startAutoUpdate() {
                if (updateTimer) clearInterval(updateTimer);
                updateTimer = setInterval(loadData, updateInterval);
            }
            
            $(document).ready(function() {
                // Инициализация заголовков
                orderedColumns.forEach(function(col) {
                    const th = $(`th[data-column="${col}"]`);
                    if (th.length) {
                        th.addClass('sortable');
                    }
                });
                
                // Обработка клика по заголовкам для сортировки
                $(document).on('click', 'th[data-column]', function() {
                    const column = $(this).data('column');
                    
                    if (sortColumn === column) {
                        sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
                    } else {
                        sortColumn = column;
                        sortOrder = 'asc';
                    }
                    
                    loadData();
                });
                
                // Начальная загрузка
                loadData();
                
                // Автообновление
                startAutoUpdate();
                
                // Обновление при возврате фокуса на вкладку
                $(window).on('focus', function() {
                    loadData();
                });
            });
        </script>
    </head>
    <body>
        <div class="loading-indicator">
            <div class="mini-spinner"></div>
            <span>Обновление...</span>
        </div>
        
        <div class="header">
            <h1>📊 SCADA Collector</h1>
            <div class="status-indicator">
                <div class="status-dot"></div>
                <span>В сети</span>
                <span class="last-update">Обновлено: —</span>
                <span class="update-indicator">●</span>
            </div>
        </div>
        
        <div class="container" style="position: relative;">
            
            <div class="table-wrapper">
                <table id="data-table">
                    <thead>
                        <tr>
                            {% for column in ordered_columns %}
                            <th data-column="{{ column }}">{{ column_display_names.get(column, column) }}</th>
                            {% endfor %}
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(
        template, 
        ordered_columns=ordered_columns,
        column_display_names=column_display_names
    )


def run_flask():
    """Запуск Flask в отдельном потоке"""
    web_config = CONFIG.get('web', {})
    host = web_config.get('host', '0.0.0.0')
    port = web_config.get('port', 80)
    logging.info(f"Запуск веб-сервера на {host}:{port}...")
    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# =============================================================================
# ФУНКЦИИ КОЛЛЕКТОРА
# =============================================================================
async def update_task_status(task_name, healthy=True, last_sync=None, error=None):
    """Обновление статуса задачи для healthcheck"""
    if task_status_lock:
        async with task_status_lock:
            task_status[task_name] = {
                'healthy': healthy,
                'last_sync': last_sync.isoformat() if last_sync else None,
                'last_error': str(error) if error else None,
                'updated': datetime.now().isoformat()
            }
    else:
        # Fallback для синхронного доступа
        task_status[task_name] = {
            'healthy': healthy,
            'last_sync': last_sync.isoformat() if last_sync else None,
            'last_error': str(error) if error else None,
            'updated': datetime.now().isoformat()
        }


async def get_cached_rectime(table_name):
    """Получение закэшированного RECTIME"""
    if rectime_cache_lock:
        async with rectime_cache_lock:
            cached = rectime_cache.get(table_name)
            if cached:
                return cached['rectime']
    else:
        cached = rectime_cache.get(table_name)
        if cached:
            return cached['rectime']
    return None


async def set_cached_rectime(table_name, rectime):
    """Сохранение RECTIME в кэш"""
    if rectime_cache_lock:
        async with rectime_cache_lock:
            rectime_cache[table_name] = {
                'rectime': rectime,
                'updated': datetime.now()
            }
    else:
        rectime_cache[table_name] = {
            'rectime': rectime,
            'updated': datetime.now()
        }


async def send_telegram_message(message, force=False, session=None):
    """
    Асинхронная отправка сообщения в Telegram с rate limiting
    
    Args:
        message: текст сообщения
        force: принудительная отправка (игнорирует rate limit)
        session: aiohttp.ClientSession (создается автоматически если None)
    """
    tg_config = CONFIG.get('telegram', {})
    if not tg_config.get('bot_token') or not tg_config.get('chat_id'):
        return
    
    # Проверка rate limit (если не force)
    if not force and not await telegram_rate_limiter.can_send():
        logging.debug(f"Telegram rate limited, сообщение подавлено: {message[:50]}...")
        return
        
    url = f"https://api.telegram.org/bot{tg_config['bot_token']}/sendMessage"
    payload = {'chat_id': tg_config['chat_id'], 'text': message}
    
    # Создаем сессию если не передана
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    
    try:
        async with session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
            response.raise_for_status()
            await telegram_rate_limiter.record_sent()
            logging.info(f"Telegram: {message}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logging.error(f"Ошибка отправки в Telegram: {e}")
    finally:
        if close_session:
            await session.close()


async def connect_to_mssql_async(server, database, uid, pwd):
    """
    Асинхронное подключение к MSSQL с экспоненциальным backoff через aioodbc
    
    Args:
        server: сервер БД
        database: имя БД
        uid: пользователь
        pwd: пароль
    """
    connection_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
    )
    
    async def do_connect():
        conn = await aioodbc.connect(dsn=connection_str, timeout=30)
        await register_connection(conn)
        return conn
    
    return await retry_with_backoff_async(
        do_connect,
        max_retries=0,  # Бесконечные попытки
        base_delay=1,
        max_delay=60,
        exceptions=(Exception,)
    )


# Единый Executor для синхронных операций (Firebird, TC2, файлы)
sync_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="sync")


def _get_firebird_data_sync(host, port, database, table, user, password, last_sync_time, objid_filter):
    """Синхронная функция получения данных из Firebird (выполняется в executor)"""
    dsn = f'{host}/{port}:{database}'
    try:
        conn = fdb.connect(dsn=dsn, user=user, password=password)
        cursor = conn.cursor()

        query = f"SELECT * FROM {table} WHERE RECTIME > ? AND OBJID = ?"
        cursor.execute(query, (last_sync_time, objid_filter))

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        return columns, rows

    except fdb.DatabaseError as e:
        logging.error(f"Ошибка Firebird {host}: {e}")
        return None, None
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        return None, None


async def get_firebird_data_with_headers(host, port, database, table, user, password, last_sync_time, objid_filter):
    """Асинхронная обертка для получения данных из Firebird"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        sync_executor,
        _get_firebird_data_sync,
        host, port, database, table, user, password, last_sync_time, objid_filter
    )


async def get_last_sync_time_async(cursor, table, use_cache=True):
    """Асинхронное получение времени последней синхронизации (с кэшем)"""
    if use_cache:
        cached = await get_cached_rectime(table)
        if cached:
            return cached
    
    try:
        await cursor.execute(f"SELECT MAX(RECTIME) FROM {table}")
        row = await cursor.fetchone()
        last_sync_time = row[0] if row and len(row) > 0 else None
        
        result = last_sync_time if last_sync_time else datetime(1900, 1, 1)
        
        if last_sync_time:
            await set_cached_rectime(table, result)
        
        return result
    except Exception as e:
        logging.error(f"Ошибка получения времени синхронизации: {e}")
        return datetime(1900, 1, 1)


def process_row(row, columns):
    """Обработка строки данных"""
    processed_row = {}
    for idx, col in enumerate(columns):
        processed_row[col] = row[idx] if idx < len(row) and row[idx] is not None else ''
    processed_row['ObjectId'] = processed_row.pop('OBJID', 1)
    return processed_row


async def insert_into_mssql_async(cursor, conn, table, data, columns, firebird_host, firebird_table):
    """Асинхронная батчевая вставка данных в MSSQL"""
    if 'ObjectId' not in columns:
        columns.append('ObjectId')

    columns_str = ", ".join([f"[{col}]" for col in columns])
    values_str = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str})"

    all_values = []
    max_rectime = None
    for row in data:
        processed_row = process_row(row, columns)
        values = tuple(processed_row.get(col, '') for col in columns)
        all_values.append(values)
        
        rectime = processed_row.get('RECTIME')
        if rectime and (max_rectime is None or rectime > max_rectime):
            max_rectime = rectime

    if all_values:
        try:
            await cursor.executemany(sql, all_values)
            await conn.commit()
            logging.info(f"Firebird: {len(all_values)} строк {firebird_host}:{firebird_table} -> {table}")
            
            if max_rectime:
                await set_cached_rectime(table, max_rectime)
                
        except Exception as e:
            if 'IntegrityError' in str(type(e)) or 'duplicate' in str(e).lower():
                await conn.rollback()
                inserted_rows = 0
                for values in all_values:
                    try:
                        await cursor.execute(sql, values)
                        inserted_rows += 1
                    except Exception:
                        pass
                await conn.commit()
                if inserted_rows > 0:
                    logging.info(f"Firebird: {inserted_rows} строк {firebird_host}:{firebird_table} -> {table}")
            else:
                raise


async def check_and_notify_async(table_name, last_update_time, telegram_session=None):
    """Асинхронная проверка и отправка уведомления если данные устарели"""
    notification_timeout = CONFIG.get('notification_timeout', 7200)
    
    lock_ctx = notifications_lock if notifications_lock else asyncio.Lock() if hasattr(asyncio, 'Lock') else None
    if lock_ctx:
        async with lock_ctx:
            await _check_and_notify_logic(table_name, last_update_time, notification_timeout, telegram_session)
    else:
        await _check_and_notify_logic(table_name, last_update_time, notification_timeout, telegram_session)


async def _check_and_notify_logic(table_name, last_update_time, notification_timeout, telegram_session):
    """Логика проверки уведомлений"""
    if last_update_time:
        if table_name in sent_notifications:
            if sent_notifications[table_name][1] < last_update_time:
                sent_notifications[table_name] = (False, last_update_time)
        else:
            sent_notifications[table_name] = (False, last_update_time)

        if (datetime.now() - last_update_time).total_seconds() > notification_timeout and not sent_notifications[table_name][0]:
            message = f"Данные в таблице {table_name} не обновлялись более {notification_timeout // 3600} часов.\nhttp://scada.veoliaenergy.uz/"
            await send_telegram_message(message, session=telegram_session)
            sent_notifications[table_name] = (True, last_update_time)


async def run_sync_mssql_async(sync_config, telegram_session):
    """Асинхронная задача синхронизации MSSQL -> MSSQL"""
    source_server = sync_config['source_server']
    source_database = sync_config['source_db']
    source_table = sync_config['source_table']
    source_login = sync_config['source_user']
    source_password = sync_config['source_pass']
    
    db_config = CONFIG['database']
    target_server = db_config['server']
    target_database = db_config['database']
    target_table = sync_config['target_table']
    target_login = db_config['username']
    target_password = db_config['password']

    task_name = f"mssql_{source_table}"
    sync_interval = CONFIG.get('sync_interval', 5)
    
    source_conn = None
    target_conn = None
    source_cursor = None
    target_cursor = None
    retry_delay = 1

    while not shutdown_event.is_set():
        try:
            # Подключение с переиспользованием
            if source_conn is None:
                source_conn = await connect_to_mssql_async(source_server, source_database, source_login, source_password)
                source_cursor = await source_conn.cursor()
            if target_conn is None:
                target_conn = await connect_to_mssql_async(target_server, target_database, target_login, target_password)
                target_cursor = await target_conn.cursor()

            # Получаем максимальное время в целевой таблице (с кэшем)
            max_rectime = await get_last_sync_time_async(target_cursor, f"[dbo].[{target_table}]")

            # Проверяем уведомления
            await source_cursor.execute(f"SELECT MAX(RECTIME) FROM [dbo].[{source_table}]")
            row = await source_cursor.fetchone()
            last_update_time = row[0] if row and len(row) > 0 else None
            await check_and_notify_async(source_table, last_update_time, telegram_session)

            # Получаем новые данные из источника
            source_query = f"""
            SELECT [ObjectId], [ID], [OBJID], [RECTIME], [T1], [T2], [T3], [T4], 
                   [T5], [T6], [V1], [V2], [P1], [P2], [T7], [T8], 
                   [V3], [V4], [V5], [P3], [P4], [H1], [H2], [H3], [H4]
            FROM [dbo].[{source_table}]
            WHERE [RECTIME] > ?
            ORDER BY [RECTIME] ASC
            """
            await source_cursor.execute(source_query, max_rectime)
            rows_to_insert = await source_cursor.fetchall()

            if rows_to_insert:
                insert_query = f"""
                INSERT INTO [dbo].[{target_table}] (
                    [ObjectId], [ID], [OBJID], [RECTIME], [T1], [T2], [T3], [T4], 
                    [T5], [T6], [V1], [V2], [P1], [P2], [T7], [T8], 
                    [V3], [V4], [V5], [P3], [P4], [H1], [H2], [H3], [H4]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                await target_cursor.executemany(insert_query, rows_to_insert)
                await target_conn.commit()
                logging.info(f"MSSQL: {len(rows_to_insert)} строк {source_table} -> {target_table}")
                
                last_row_rectime = rows_to_insert[-1][3] if len(rows_to_insert) > 0 else None
                if last_row_rectime:
                    await set_cached_rectime(f"[dbo].[{target_table}]", last_row_rectime)

            await update_task_status(task_name, healthy=True, last_sync=datetime.now())
            retry_delay = 1  # Сброс задержки при успехе
            
            # Ожидание с проверкой shutdown
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=sync_interval)
                break
            except asyncio.TimeoutError:
                pass

        except Exception as e:
            logging.error(f"Ошибка синхронизации MSSQL {source_table}: {e}")
            await update_task_status(task_name, healthy=False, error=e)
            
            # Закрытие соединений
            await close_connection_safe(source_conn)
            await close_connection_safe(target_conn)
            
            source_conn = target_conn = source_cursor = target_cursor = None
            
            # Экспоненциальный backoff
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=min(retry_delay, 60))
                break
            except asyncio.TimeoutError:
                retry_delay = min(retry_delay * 2, 60)


async def run_sync_firebird_async(sync_config, telegram_session):
    """Асинхронная задача синхронизации Firebird -> MSSQL"""
    firebird_host = sync_config['host']
    firebird_port = sync_config['port']
    firebird_db = sync_config['database']
    firebird_table = sync_config['table']
    firebird_user = sync_config['user']
    firebird_password = sync_config['password']
    mssql_table = sync_config['target_table']
    objid_filter = sync_config['objid']
    
    db_config = CONFIG['database']
    mssql_server = db_config['server']
    mssql_db = db_config['database']
    mssql_uid = db_config['username']
    mssql_pwd = db_config['password']

    task_name = f"firebird_{mssql_table.replace('dbo.', '')}"
    sync_interval = CONFIG.get('sync_interval', 5)
    
    mssql_conn = None
    mssql_cursor = None
    retry_delay = 1

    while not shutdown_event.is_set():
        try:
            if mssql_conn is None:
                mssql_conn = await connect_to_mssql_async(mssql_server, mssql_db, mssql_uid, mssql_pwd)
                mssql_cursor = await mssql_conn.cursor()

            last_sync_time = await get_last_sync_time_async(mssql_cursor, mssql_table)
            await check_and_notify_async(mssql_table, last_sync_time, telegram_session)

            headers, data = await get_firebird_data_with_headers(
                firebird_host, firebird_port, firebird_db, firebird_table,
                firebird_user, firebird_password, last_sync_time, objid_filter
            )

            if headers and data:
                await insert_into_mssql_async(mssql_cursor, mssql_conn, mssql_table, data, headers, firebird_host, firebird_table)

            await update_task_status(task_name, healthy=True, last_sync=datetime.now())
            retry_delay = 1
            
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=sync_interval)
                break
            except asyncio.TimeoutError:
                pass

        except Exception as e:
            logging.error(f"Ошибка синхронизации Firebird {firebird_table}: {e}")
            await update_task_status(task_name, healthy=False, error=e)
            
            await close_connection_safe(mssql_conn)
            mssql_conn = mssql_cursor = None
            
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=min(retry_delay, 60))
                break
            except asyncio.TimeoutError:
                retry_delay = min(retry_delay * 2, 60)


# =============================================================================
# TC2 HEATING PROCESSOR (ASYNC)
# =============================================================================
def _connect_to_network_share(share_path, username, password):
    r"""
    Подключение к сетевой папке с использованием учетных данных через net use
    
    Args:
        share_path: UNC путь к сетевой папке (например, \\192.168.1.1\share)
        username: Имя пользователя (например, domain\user или user@domain.com)
        password: Пароль
    
    Returns:
        bool: True если подключение успешно, False в противном случае
    """
    try:
        # Извлекаем базовый путь к шаре (первые два уровня UNC пути)
        # Например: \\192.168.230.241\c$ из \\192.168.230.241\c$\hscmt\Ozbekiston\cal\H
        parts = share_path.replace('\\', '/').strip('/').split('/')
        if len(parts) >= 2:
            base_share = f"\\\\{parts[0]}\\{parts[1]}"
        else:
            base_share = share_path
        
        # Формируем команду net use
        # Используем /persistent:no чтобы не сохранять подключение после перезагрузки
        # Для доменного пользователя в формате user@domain.com используем его как есть
        # Для формата domain\user также используем как есть
        cmd = [
            'net', 'use', base_share,
            f'/user:{username}',
            password,
            '/persistent:no'
        ]
        
        # Выполняем команду (скрываем вывод пароля)
        # Используем shell=True для правильной обработки специальных символов в пароле
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            shell=False,  # False для безопасности, но может потребоваться shell=True для некоторых символов
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        if result.returncode == 0:
            logging.info(f"TC2: Успешное подключение к сетевой папке: {base_share}")
            return True
        else:
            # Проверяем, может быть папка уже подключена
            error_text = result.stderr.lower() if result.stderr else ''
            if 'already connected' in error_text or 'уже подключен' in error_text:
                logging.info(f"TC2: Сетевая папка уже подключена: {base_share}")
                return True
            else:
                logging.warning(f"TC2: Ошибка подключения к сетевой папке {base_share}: {result.stderr}")
                return False
                
    except subprocess.TimeoutExpired:
        logging.error(f"TC2: Таймаут при подключении к сетевой папке: {share_path}")
        return False
    except Exception as e:
        logging.error(f"TC2: Исключение при подключении к сетевой папке: {e}", exc_info=True)
        return False


def _read_excel_file_sync(file_path, skip_footer_rows, last_db_record):
    """Синхронная функция чтения Excel файла"""
    try:
        if not file_path.exists():
            return None

        # Создаем временную копию файла
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            shutil.copy2(file_path, temp_path)
            df = pd.read_excel(temp_path, skipfooter=skip_footer_rows)

            if df.empty:
                return None

            # Маппинг колонок
            column_mapping = {
                'дата и время проверки': 'check_datetime',
                'температура подачи\n(℃)': 'temperature_supply',
                'температура возврата\n(℃)': 'temperature_return',
                'Температура ХВС\n(℃)': 'temperature_cold_water',
                'Температура ХВС_x000A_(℃)': 'temperature_cold_water',
                'расход подачи\n(㎥)': 'flow_supply',
                'расход возврата\n(㎥)': 'flow_return',
                'разница\n(㎥)': 'flow_difference',
                'период Гкал\n(Gcal)': 'period_gcal',
                'Период нагрева\n(Gcal)': 'period_heating_gcal',
                'давление подачи\n(bar)': 'pressure_supply',
                'давление возврата\n(bar)': 'pressure_return'
            }

            existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=existing_columns)

            # Преобразуем дату/время - ищем колонку с датой
            date_col = None
            if 'check_datetime' in df.columns:
                date_col = 'check_datetime'
            else:
                # Ищем колонку с датой вручную
                for col in df.columns:
                    if 'дата' in col.lower() or 'время' in col.lower() or 'datetime' in col.lower():
                        date_col = col
                        df = df.rename(columns={col: 'check_datetime'})
                        logging.debug(f"TC2: Найдена колонка с датой: {col} -> check_datetime")
                        break
            
            if not date_col:
                logging.error(f"TC2: Колонка с датой не найдена в файле {file_path.name}. Доступные колонки: {list(df.columns)}")
                return None

            # Преобразуем дату/время
            df['check_datetime'] = pd.to_datetime(df['check_datetime'], errors='coerce')
            initial_count = len(df)
            df = df.dropna(subset=['check_datetime'])
            if len(df) < initial_count:
                logging.warning(f"TC2: Удалено {initial_count - len(df)} строк с некорректными датами из {file_path.name}")

            # Преобразуем числовые колонки
            numeric_columns = [
                'temperature_supply', 'temperature_return', 'temperature_cold_water',
                'flow_supply', 'flow_return', 'flow_difference',
                'period_gcal', 'period_heating_gcal',
                'pressure_supply', 'pressure_return'
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace(',', '.', regex=False)
                        .pipe(pd.to_numeric, errors='coerce')
                    )

            df['file_name'] = file_path.name

            # Логируем информацию о файле
            if len(df) > 0:
                min_date = df['check_datetime'].min()
                max_date = df['check_datetime'].max()
                logging.debug(f"TC2: Файл {file_path.name} - {len(df)} записей, диапазон: {min_date} - {max_date}")

            # Фильтруем только новые записи
            # Сохраняем оригинальный максимум до фильтрации
            file_max_before_filter = df['check_datetime'].max() if len(df) > 0 else None
            
            if last_db_record:
                initial_rows = len(df)
                last_db_dt = pd.to_datetime(last_db_record)
                # Используем строгое сравнение > для фильтрации
                df = df[df['check_datetime'] > last_db_dt]
                filtered_rows = initial_rows - len(df)
                if filtered_rows > 0:
                    logging.info(f"TC2: Отфильтровано {filtered_rows} записей из {file_path.name} (уже есть в БД, последняя: {last_db_record})")
                if len(df) > 0:
                    new_min = df['check_datetime'].min()
                    new_max = df['check_datetime'].max()
                    logging.info(f"TC2: Осталось {len(df)} новых записей для обработки из {file_path.name} (диапазон: {new_min} - {new_max})")
                else:
                    # Логируем, почему не осталось записей
                    if initial_rows > 0 and file_max_before_filter is not None:
                        time_diff = (file_max_before_filter - last_db_dt).total_seconds()
                        if abs(time_diff) < 60:  # Разница менее минуты
                            logging.warning(f"TC2: Все записи отфильтрованы. Последняя в файле: {file_max_before_filter}, в БД: {last_db_record}, разница: {time_diff:.0f} сек")
                        elif time_diff <= 0:
                            logging.debug(f"TC2: Все записи отфильтрованы. Последняя в файле: {file_max_before_filter} <= последней в БД: {last_db_record}")
                        else:
                            logging.debug(f"TC2: Все записи отфильтрованы. Последняя в файле: {file_max_before_filter}, в БД: {last_db_record}, разница: {time_diff/3600:.2f} ч")

            return df

        finally:
            try:
                os.unlink(temp_path)
            except:
                pass

    except Exception as e:
        logging.error(f"Ошибка чтения Excel файла {file_path}: {e}")
        return None


async def read_excel_file_async(file_path, skip_footer_rows, last_db_record):
    """Асинхронная обертка для чтения Excel"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        sync_executor,
        _read_excel_file_sync,
        file_path, skip_footer_rows, last_db_record
    )




async def save_tc2_to_sqlserver_async(cursor, conn, df, config):
    """Асинхронная вставка данных TC2 в SQL Server
    
    Returns:
        tuple: (количество вставленных строк, максимальное время RECTIME вставленных записей)
    """
    try:
        if df is None or df.empty:
            return 0, None

        tmp = df.copy()
        tmp['check_datetime'] = pd.to_datetime(tmp['check_datetime'], errors='coerce')
        tmp = tmp.dropna(subset=['check_datetime'])

        rows = []
        max_rectime = None
        obj = config.get('object_id', 1)
        idv = config.get('id_value', 1)
        ojd = config.get('objid_value', 1)

        for _, r in tmp.iterrows():
            rectime = r['check_datetime'].to_pydatetime()
            if max_rectime is None or rectime > max_rectime:
                max_rectime = rectime
                
            rows.append((
                obj,                                         # ObjectId
                idv,                                         # ID
                ojd,                                         # OBJID
                rectime,                                     # RECTIME
                r.get('temperature_supply'),                 # T1
                r.get('temperature_return'),                 # T2
                r.get('temperature_cold_water'),             # T3
                None,                                        # T4
                None,                                        # T5
                None,                                        # T6
                r.get('flow_supply'),                        # V1
                r.get('flow_return'),                        # V2
                r.get('pressure_supply'),                    # P1
                r.get('pressure_return'),                    # P2
                None,                                        # T7
                None,                                        # T8
                r.get('flow_difference'),                    # V3
                None,                                        # V4
                None,                                        # V5
                None,                                        # P3
                None,                                        # P4
                r.get('period_gcal'),                        # H1
                r.get('period_heating_gcal'),                # H2
                None,                                        # H3
                None                                         # H4
            ))

        if not rows:
            return 0, None

        sql = f"""
        INSERT INTO {config['target_table']}
        (ObjectId, ID, OBJID, RECTIME, T1, T2, T3, T4, T5, T6, V1, V2, P1, P2, T7, T8, V3, V4, V5, P3, P4, H1, H2, H3, H4)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            await cursor.executemany(sql, rows)
            await conn.commit()
            inserted = len(rows)
            logging.info(f"TC2: Вставлено {inserted} строк в {config['target_table']}, максимальное время: {max_rectime}")
            return inserted, max_rectime
        except Exception as e:
            if 'IntegrityError' in str(type(e)) or 'duplicate' in str(e).lower():
                await conn.rollback()
                success_count = 0
                success_max_rectime = None
                for row in rows:
                    try:
                        await cursor.execute(sql, row)
                        success_count += 1
                        row_rectime = row[3]  # RECTIME находится на позиции 3
                        if success_max_rectime is None or row_rectime > success_max_rectime:
                            success_max_rectime = row_rectime
                    except Exception:
                        pass
                await conn.commit()
                logging.info(f"TC2: Вставлено {success_count} строк (дубликаты пропущены), максимальное время: {success_max_rectime}")
                return success_count, success_max_rectime
            else:
                raise

    except Exception as e:
        logging.error(f"Ошибка вставки TC2 в SQL Server: {e}", exc_info=True)
        return 0, None


async def run_tc2_processor_async(config, telegram_session):
    """Асинхронная задача обработки TC2 Excel файлов"""
    if not config.get('enabled', True):
        logging.info("TC2 процессор отключен в конфигурации")
        return

    task_name = "tc2_processor"
    files_directory = Path(config.get('files_directory', r'\\192.168.230.241\c$\hscmt\Ozbekiston\cal\H'))
    monitor_interval = config.get('monitor_interval', 30)
    days_to_search = config.get('days_to_search', 30)
    skip_footer_rows = config.get('skip_footer_rows', 1)
    target_table = config.get('target_table', 'dbo.Dynamic_TC2')

    db_config = CONFIG['database']
    mssql_server = db_config['server']
    mssql_db = db_config['database']
    mssql_uid = db_config['username']
    mssql_pwd = db_config['password']

    mssql_conn = None
    mssql_cursor = None
    last_db_record = None
    network_available = False
    network_check_counter = 0
    retry_delay = 1
    
    # Отслеживание времени последней проверки файлов (для оптимизации)
    files_last_check = {}  # {file_name: last_check_time}
    file_check_interval = config.get('file_check_interval', 3600)  # Интервал проверки файла в секундах (по умолчанию 1 час)

    logging.info(f"TC2 процессор инициализирован. Каталог: {files_directory}")
    logging.info(f"TC2: Интервал проверки файлов: {file_check_interval/60:.0f} минут")
    
    # Получаем учетные данные для сетевой папки из конфигурации
    service_config = CONFIG.get('service', {})
    network_username = service_config.get('run_as_user', '')
    network_password = service_config.get('run_as_password', '')
    network_connected = False  # Флаг подключения к сетевой папке

    while not shutdown_event.is_set():
        try:
            # Подключение к БД
            if mssql_conn is None:
                mssql_conn = await connect_to_mssql_async(mssql_server, mssql_db, mssql_uid, mssql_pwd)
                mssql_cursor = await mssql_conn.cursor()
                last_db_record = await get_last_sync_time_async(mssql_cursor, target_table, use_cache=False)
                if last_db_record and last_db_record.year > 1900:
                    logging.info(f"TC2: Последняя запись в БД: {last_db_record}")
                else:
                    logging.info(f"TC2: БД пуста или последняя запись: {last_db_record}")
            
            # Логируем текущее состояние в начале каждого цикла
            logging.debug(f"TC2: Начало цикла обработки. Последняя запись в БД: {last_db_record}, сеть доступна: {network_available}")

            # Проверка доступности директории
            network_check_counter += 1
            check_interval = max(1, config.get('network_check_interval', 3600) // monitor_interval)

            if not network_available or network_check_counter >= check_interval:
                network_check_counter = 0
                try:
                    # Подключение к сетевой папке с учетными данными из config.json
                    if str(files_directory).startswith('\\\\'):
                        # Если папка недоступна и есть учетные данные, пытаемся подключиться
                        if not network_connected or not files_directory.exists():
                            if network_username and network_password:
                                logging.info(f"TC2: Попытка подключения к сетевой папке с учетными данными: {network_username}")
                                network_connected = _connect_to_network_share(
                                    str(files_directory),
                                    network_username,
                                    network_password
                                )
                            else:
                                logging.warning("TC2: Учетные данные для сетевой папки не найдены в config.json (секция service)")
                    
                    # Проверяем доступность папки
                    if files_directory.exists():
                        try:
                            list(files_directory.glob("*"))
                            network_available = True
                            logging.debug(f"TC2: Сетевая папка доступна")
                        except Exception as e:
                            network_available = False
                            logging.warning(f"TC2: Ошибка доступа к содержимому папки: {e}")
                    else:
                        network_available = False
                        logging.warning(f"TC2: Сетевая папка недоступна: {files_directory}")
                except PermissionError as e:
                    network_available = False
                    logging.error(f"TC2: Ошибка доступа к сетевой папке (нет прав): {e}")
                    # Пытаемся переподключиться
                    if network_username and network_password:
                        logging.info("TC2: Попытка переподключения к сетевой папке...")
                        network_connected = _connect_to_network_share(
                            str(files_directory),
                            network_username,
                            network_password
                        )
                except Exception as e:
                    network_available = False
                    logging.debug(f"TC2: Директория недоступна: {e}")

            if not network_available:
                await update_task_status(task_name, healthy=False, error="Директория недоступна")
                await asyncio.sleep(monitor_interval)
                continue

            # Поиск файлов
            search_date = datetime.now().date() - timedelta(days=days_to_search)
            try:
                all_files = list(files_directory.glob("*TC-2.xlsx"))
            except Exception as e:
                logging.error(f"TC2: Ошибка поиска файлов: {e}")
                network_available = False
                await asyncio.sleep(monitor_interval)
                continue

            # Фильтрация файлов по дате и времени модификации
            files_to_process = []
            last_db_date = None
            if last_db_record:
                try:
                    last_db_date = pd.to_datetime(last_db_record).date()
                except:
                    pass
            
            logging.debug(f"TC2: Найдено {len(all_files)} файлов, поиск с даты {search_date}, последняя запись в БД: {last_db_record}")
            
            current_date = datetime.now().date()
            current_time = datetime.now()
            
            for file_path in all_files:
                match = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
                if match:
                    try:
                        file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                        if file_date >= search_date:
                            # Получаем время модификации файла
                            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                            time_since_modification = (current_time - file_mtime).total_seconds() / 3600
                            
                            # Всегда обрабатываем файлы текущего дня (могут обновляться)
                            # И файлы, которые новее последней записи в БД
                            should_process = False
                            
                            if file_date == current_date:
                                # Файлы текущего дня - проверяем с интервалом (чтобы не мешать записи)
                                # Проверяем, если:
                                # 1. Файл еще не проверялся
                                # 2. Прошло достаточно времени с последней проверки
                                # 3. Файл был изменен после последней записи в БД
                                file_updated_after_db = not last_db_record or file_mtime > last_db_record
                                last_check = files_last_check.get(file_path.name)
                                time_since_last_check = (current_time - last_check).total_seconds() if last_check else float('inf')
                                
                                if not last_check:
                                    # Файл еще не проверялся - обрабатываем
                                    should_process = True
                                    logging.debug(f"TC2: Файл текущего дня {file_path.name} будет обработан (первая проверка)")
                                elif time_since_last_check >= file_check_interval:
                                    # Прошло достаточно времени с последней проверки
                                    should_process = True
                                    logging.debug(f"TC2: Файл текущего дня {file_path.name} будет обработан (прошло {time_since_last_check/60:.0f} мин. с последней проверки)")
                                elif file_updated_after_db and time_since_last_check >= 300:  # Минимум 5 минут между проверками
                                    # Файл обновлен после БД, но прошло минимум 5 минут с последней проверки
                                    should_process = True
                                    logging.debug(f"TC2: Файл текущего дня {file_path.name} будет обработан (обновлен после БД, прошло {time_since_last_check/60:.0f} мин.)")
                                else:
                                    logging.debug(f"TC2: Файл {file_path.name} пропущен (проверялся {time_since_last_check/60:.1f} мин. назад, интервал: {file_check_interval/60:.0f} мин.)")
                            elif not last_db_record or not last_db_date:
                                # Если БД пуста или нет даты, обрабатываем все файлы
                                should_process = True
                            elif file_date > last_db_date:
                                # Файлы с датой новее последней записи в БД
                                should_process = True
                            elif file_date == last_db_date:
                                # Файлы с той же датой - проверяем время модификации
                                # Если файл был изменен недавно (в последние 2 часа), обрабатываем
                                if time_since_modification < 2:
                                    should_process = True
                                    logging.debug(f"TC2: Файл {file_path.name} с датой {file_date} будет обработан (изменен {time_since_modification:.1f} ч. назад)")
                            
                            if should_process:
                                files_to_process.append((file_path, file_mtime))
                    except (ValueError, OSError) as e:
                        logging.debug(f"TC2: Не удалось обработать файл {file_path.name}: {e}")
                        continue

            # Сортируем по времени модификации (сначала более свежие)
            files_to_process.sort(key=lambda x: x[1], reverse=True)
            # Извлекаем только пути к файлам
            files_to_process = [fp[0] for fp in files_to_process]
            
            if files_to_process:
                logging.info(f"TC2: Отобрано {len(files_to_process)} файлов для обработки")
                # Логируем информацию о файлах
                for file_path in files_to_process[:3]:  # Показываем первые 3
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        file_size = file_path.stat().st_size / 1024
                        logging.debug(f"TC2: Файл {file_path.name} - изменен: {file_mtime}, размер: {file_size:.1f} KB")
                    except:
                        pass

            # Обработка файлов
            processed_count = 0
            max_processed_time = last_db_record  # Отслеживаем максимальное время обработанных записей
            
            # Логируем информацию о файлах для обработки
            if files_to_process:
                logging.info(f"TC2: Начинаем обработку {len(files_to_process)} файлов (последняя запись в БД: {last_db_record})")
                for file_path in files_to_process[:5]:  # Показываем первые 5
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        file_size = file_path.stat().st_size / 1024
                        logging.info(f"TC2: Будет обработан: {file_path.name} (изменен: {file_mtime}, размер: {file_size:.1f} KB)")
                    except:
                        pass
            else:
                logging.debug(f"TC2: Файлов для обработки не найдено")
            
            for file_path in files_to_process:
                if shutdown_event.is_set():
                    break

                try:
                    # Получаем информацию о файле
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    file_size = file_path.stat().st_size / 1024
                    
                    # Проверяем, был ли файл изменен после последней записи в БД
                    file_updated_after_db = not last_db_record or file_mtime > last_db_record
                    
                    # Обновляем время последней проверки файла
                    files_last_check[file_path.name] = datetime.now()
                    
                    logging.info(f"TC2: Обработка {file_path.name} (изменен: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}, размер: {file_size:.1f} KB, последняя запись в БД: {last_db_record}, файл обновлен после БД: {file_updated_after_db})")
                    
                    df = await read_excel_file_async(file_path, skip_footer_rows, last_db_record)

                    if df is not None and not df.empty:
                        # Получаем информацию о данных в файле
                        date_col = None
                        for col in df.columns:
                            if 'дата' in col.lower() or 'время' in col.lower() or col == 'check_datetime':
                                date_col = col
                                break
                        
                        file_min_time = None
                        file_max_time = None
                        if date_col and date_col in df.columns:
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                            df_with_dates = df.dropna(subset=[date_col])
                            if len(df_with_dates) > 0:
                                file_min_time = df_with_dates[date_col].min().to_pydatetime()
                                file_max_time = df_with_dates[date_col].max().to_pydatetime()
                                logging.debug(f"TC2: {file_path.name} - данные в файле: {file_min_time} - {file_max_time}")
                        
                        inserted, max_inserted_time = await save_tc2_to_sqlserver_async(mssql_cursor, mssql_conn, df, config)
                        if inserted > 0:
                            processed_count += inserted
                            # Обновляем максимальное время обработанных записей
                            if max_inserted_time and (max_processed_time is None or max_inserted_time > max_processed_time):
                                max_processed_time = max_inserted_time
                            logging.info(f"TC2: {file_path.name} - добавлено {inserted} записей (макс. время: {max_inserted_time})")
                        else:
                            # Даже если новых записей нет, обновляем max_processed_time на максимальное время из файла
                            # чтобы не обрабатывать этот файл снова
                            if file_max_time:
                                if max_processed_time is None or file_max_time > max_processed_time:
                                    max_processed_time = file_max_time
                                
                                # Проверяем, обновлялся ли файл недавно
                                time_since_modification = (datetime.now() - file_mtime).total_seconds() / 3600
                                time_since_last_data = (datetime.now() - file_max_time).total_seconds() / 3600
                                
                                # Если файл был изменен после последней записи в БД, но данных новых нет,
                                # это может означать, что данные еще не записаны в файл
                                if file_updated_after_db and time_since_modification < 1:
                                    logging.warning(f"TC2: {file_path.name} - файл обновлен {time_since_modification:.1f} ч. назад (после последней записи в БД), но новых данных нет. Возможно, данные еще записываются в файл. Макс. время в файле: {file_max_time}")
                                elif time_since_modification < 2 and time_since_last_data > 1:
                                    logging.warning(f"TC2: {file_path.name} - файл обновлен {time_since_modification:.1f} ч. назад, но данные устарели на {time_since_last_data:.1f} ч. (макс. время в файле: {file_max_time})")
                                else:
                                    logging.info(f"TC2: {file_path.name} - новых записей нет (все уже в БД), макс. время в файле: {file_max_time}")
                            else:
                                # Если файл обновлен, но нет данных - возможно файл еще обновляется
                                if file_updated_after_db and time_since_modification < 1:
                                    logging.warning(f"TC2: {file_path.name} - файл обновлен {time_since_modification:.1f} ч. назад, но данных нет. Файл может еще обновляться.")
                                else:
                                    logging.info(f"TC2: {file_path.name} - новых записей нет (файл пуст или нет дат)")
                    else:
                        logging.info(f"TC2: {file_path.name} - файл пуст или нет новых данных")
                except Exception as e:
                    logging.error(f"TC2: Ошибка обработки {file_path.name}: {e}", exc_info=True)

            # Обновляем last_db_record после обработки всех файлов
            if max_processed_time:
                # Получаем актуальное максимальное время из БД
                new_last_record = await get_last_sync_time_async(mssql_cursor, target_table, use_cache=False)
                if new_last_record:
                    if last_db_record is None or new_last_record > last_db_record:
                        last_db_record = new_last_record
                        # Обновляем кэш
                        await set_cached_rectime(target_table, last_db_record)
                        logging.info(f"TC2: Обновлена последняя запись в БД: {last_db_record}")
                    else:
                        logging.debug(f"TC2: Последняя запись в БД не изменилась: {last_db_record}")

            # Итоговое логирование
            if processed_count > 0:
                logging.info(f"TC2: Всего обработано {processed_count} записей")
            elif files_to_process:
                logging.info(f"TC2: Обработано {len(files_to_process)} файлов, новых записей не найдено")
            else:
                logging.debug(f"TC2: Файлов для обработки не найдено (найдено {len(all_files)} файлов всего)")

            # Периодически обновляем last_db_record из БД, даже если файлы не обрабатывались
            # Это нужно для отслеживания изменений, сделанных вручную или другими процессами
            if mssql_conn and mssql_cursor:
                try:
                    current_db_record = await get_last_sync_time_async(mssql_cursor, target_table, use_cache=False)
                    if current_db_record:
                        # Проверяем, не устарели ли данные
                        time_since_last = (datetime.now() - current_db_record).total_seconds() / 3600
                        
                        if current_db_record != last_db_record:
                            if last_db_record is None or current_db_record > last_db_record:
                                last_db_record = current_db_record
                                await set_cached_rectime(target_table, last_db_record)
                                logging.info(f"TC2: Обновлена последняя запись из БД: {last_db_record} (устарела на {time_since_last:.1f} ч.)")
                        else:
                            # Данные не изменились, но проверяем устаревание
                            if time_since_last > 1:
                                logging.warning(f"TC2: Данные в БД устарели на {time_since_last:.1f} часов (последняя запись: {current_db_record})")
                            else:
                                logging.debug(f"TC2: Последняя запись в БД: {current_db_record} (актуальна, устарела на {time_since_last:.1f} ч.)")
                except Exception as e:
                    logging.debug(f"TC2: Ошибка при обновлении last_db_record: {e}")

            await update_task_status(task_name, healthy=True, last_sync=datetime.now())
            retry_delay = 1

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=monitor_interval)
                break
            except asyncio.TimeoutError:
                pass

        except Exception as e:
            logging.error(f"TC2: Ошибка процессора: {e}")
            await update_task_status(task_name, healthy=False, error=e)

            await close_connection_safe(mssql_conn)
            mssql_conn = mssql_cursor = None

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=min(retry_delay, 60))
                break
            except asyncio.TimeoutError:
                retry_delay = min(retry_delay * 2, 60)


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================
async def async_main():
    """Асинхронная главная функция"""
    global rectime_cache_lock, notifications_lock, task_status_lock
    
    # Инициализация asyncio locks (можно только внутри event loop)
    rectime_cache_lock = asyncio.Lock()
    notifications_lock = asyncio.Lock()
    task_status_lock = asyncio.Lock()
    
    # Инициализация lock для rate limiter
    telegram_rate_limiter.lock = asyncio.Lock()
    
    logging.info("=" * 60)
    logging.info("Запуск SCADA Collector + Web Server (AsyncIO)")
    logging.info(f"Конфигурация загружена из config.json")
    logging.info("=" * 60)
    
    # Запуск Flask в отдельном потоке
    flask_thread = threading.Thread(target=run_flask, daemon=True, name="flask")
    flask_thread.start()
    logging.info("Веб-сервер запущен")

    # Создаем общий aiohttp сессию для всех задач
    telegram_session = aiohttp.ClientSession()

    # Создаем список задач для синхронизации
    tasks = []

    # Запуск синхронизаций MSSQL -> MSSQL
    sync_mssql = CONFIG.get('sync_mssql', [])
    for sync_config in sync_mssql:
        task = asyncio.create_task(
            run_sync_mssql_async(sync_config, telegram_session),
            name=f"mssql_{sync_config['source_table']}"
        )
        tasks.append(task)
    logging.info(f"Запущено {len(sync_mssql)} задач MSSQL синхронизации")

    # Запуск синхронизаций Firebird -> MSSQL
    sync_firebird = CONFIG.get('sync_firebird', [])
    for sync_config in sync_firebird:
        task = asyncio.create_task(
            run_sync_firebird_async(sync_config, telegram_session),
            name=f"firebird_{sync_config['target_table']}"
        )
        tasks.append(task)
    logging.info(f"Запущено {len(sync_firebird)} задач Firebird синхронизации")

    # Запуск TC2 процессора (если включен)
    tc2_config = CONFIG.get('tc2_processor', {})
    if tc2_config.get('enabled', False):
        task = asyncio.create_task(
            run_tc2_processor_async(tc2_config, telegram_session),
            name="tc2_processor"
        )
        tasks.append(task)
        logging.info("Запущена задача TC2 процессора")

    logging.info("Все сервисы запущены. Нажмите Ctrl+C для остановки.")
    logging.info("Healthcheck: http://localhost/health")

    # Ожидание завершения всех задач или shutdown
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        logging.info("Получен KeyboardInterrupt, завершение работы...")
    finally:
        # Закрываем сессию Telegram
        await telegram_session.close()
        logging.info("Сессия Telegram закрыта")


def main():
    """Точка входа - запуск async главной функции"""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logging.info("Завершение работы...")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

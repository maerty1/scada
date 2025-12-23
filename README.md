# SCADA Collector

Система сбора и синхронизации данных SCADA в центральную базу данных.

## Установка

1. Клонируйте репозиторий:
   ```bash
   git clone <repository-url>
   cd python_collector
   ```

2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

3. Настройте конфигурацию:
   ```bash
   copy config.json.example config.json
   ```
   Отредактируйте `config.json` и укажите реальные значения для:
   - Параметров подключения к базе данных
   - Telegram бота (chat_id, bot_token)
   - Источников данных (sync_mssql, sync_firebird)
   - Сетевых путей (tc2_processor)
   - Учетных данных для службы Windows (service) - опционально

## Конфигурация

Все настройки находятся в файле `config.json`. Пример конфигурации можно найти в `config.json.example`.

### Важные секции:

- **database** - параметры подключения к целевой базе данных
- **telegram** - настройки Telegram бота для уведомлений
- **sync_mssql** - список источников MSSQL для синхронизации
- **sync_firebird** - список источников Firebird для синхронизации
- **tc2_processor** - настройки обработчика TC2 Excel файлов
- **service** - учетные данные для запуска службы Windows (опционально, используется скриптом setup_service_user.bat)

## Запуск

### Ручной запуск:
```bash
python collector.py
```

### Установка как Windows служба:
См. [SERVICE_SETUP.md](SERVICE_SETUP.md) для подробных инструкций.

## Безопасность

⚠️ **ВАЖНО**: Файл `config.json` содержит чувствительные данные (пароли, токены) и **НЕ должен** попадать в систему контроля версий.

- Файл `config.json` уже добавлен в `.gitignore`
- Используйте `config.json.example` как шаблон для настройки
- Никогда не коммитьте реальный `config.json` в репозиторий

## Структура проекта

```
python_collector/
├── collector.py          # Основной файл приложения
├── config.json           # Конфигурация (не в git)
├── config.json.example   # Пример конфигурации
├── .gitignore           # Игнорируемые файлы
├── install_service.bat   # Установка Windows службы
├── uninstall_service.bat # Удаление Windows службы
└── SERVICE_SETUP.md      # Документация по настройке службы
```

## Лицензия

[Укажите лицензию]


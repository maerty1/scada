# Настройка Git Remote для проекта SCADA Collector

## Текущая ситуация
- Локальный репозиторий настроен
- Remote URL исправлен на: `https://github.com/maerty1/scada.git`
- Репозиторий на GitHub еще не создан или недоступен

## Шаги для настройки

### 1. Создайте репозиторий на GitHub
- Перейдите на https://github.com/new
- Название: `scada`
- Описание: "SCADA Collector - система сбора и синхронизации данных SCADA"
- Выберите **Private** (рекомендуется для проектов с конфигурацией)
- **НЕ** добавляйте README, .gitignore или лицензию (они уже есть в проекте)
- Нажмите "Create repository"

### 2. После создания репозитория выполните:

```bash
# Проверьте текущий remote
git remote -v

# Если нужно изменить URL (если репозиторий называется по-другому)
git remote set-url scada https://github.com/maerty1/scada.git

# Добавьте все файлы (если еще не добавлены)
git add .

# Создайте коммит (если еще не создан)
git commit -m "feat: авторизация в сетевую папку через приложение и безопасность конфигурации

- Добавлена авторизация в сетевую папку через net use
- Учетные данные перенесены в config.json (секция service)
- Создан config.json.example и .gitignore для безопасной публикации
- Добавлена документация (README.md, SERVICE_SETUP.md)"

# Отправьте код в репозиторий и настройте upstream
git push -u scada main
```

### 3. Если используете SSH (альтернатива):

```bash
# Измените URL на SSH формат
git remote set-url scada git@github.com:maerty1/scada.git

# Затем push
git push -u scada main
```

### 4. Проверка

После успешного push:
```bash
git branch -vv
# Должно показать: main 564ca55 [scada/main] It seems
```

## Важно

⚠️ **Убедитесь, что `config.json` НЕ попадет в репозиторий!**

Проверьте перед push:
```bash
git status
# config.json НЕ должен быть в списке изменений
```

Если `config.json` попал в индекс:
```bash
git reset HEAD config.json
git rm --cached config.json
```


@echo off
chcp 1251
REM Настройка службы для запуска от имени доменного пользователя
REM ЗАПУСКАТЬ ОТ ИМЕНИ АДМИНИСТРАТОРА!

echo ========================================
echo Настройка службы SCADA_Collector
echo для запуска от имени доменного пользователя
echo ========================================
echo.

REM Проверка наличия config.json
if not exist "config.json" (
    echo [ERROR] Файл config.json не найден!
    pause
    exit /b 1
)

REM Чтение учетных данных из config.json через Python
for /f "tokens=1,2 delims=|" %%a in ('python read_config.py 2^>nul') do (
    set SERVICE_USER=%%a
    set SERVICE_PASSWORD=%%b
)

if "%SERVICE_USER%"=="" (
    echo [ERROR] Не найдены учетные данные в config.json!
    echo.
    echo Убедитесь, что в config.json есть секция "service" с полями:
    echo   "service": {
    echo     "run_as_user": "shabalin.ev@vet.uz",
    echo     "run_as_password": "пароль"
    echo   }
    echo.
    echo Или введите учетные данные вручную:
    set /p SERVICE_USER="Введите имя пользователя (domain\user или user@domain.com): "
    set /p SERVICE_PASSWORD="Введите пароль: "
)

if "%SERVICE_USER%"=="" (
    echo [ERROR] Имя пользователя не указано!
    pause
    exit /b 1
)

echo Пользователь: %SERVICE_USER%
echo.

REM Остановка службы
echo 1. Остановка службы...
.\nssm.exe stop SCADA_Collector
timeout /t 3 /nobreak >nul

REM Настройка запуска от имени пользователя
echo.
echo 2. Настройка запуска от имени пользователя...
.\nssm.exe set SCADA_Collector ObjectName %SERVICE_USER% %SERVICE_PASSWORD%

if %errorlevel% neq 0 (
    echo [ERROR] Ошибка настройки пользователя!
    echo Возможно, неправильный пароль или пользователь не имеет прав.
    pause
    exit /b 1
)

REM Получаем пути к Python для PYTHONPATH
for /f "delims=" %%i in ('where python') do set PYTHON_DIR=%%~dpi
set PYTHON_DIR=%PYTHON_DIR:~0,-1%
set USER_SITE=%APPDATA%\Python\Python312\site-packages
set SYSTEM_SITE=%PYTHON_DIR%Lib\site-packages

REM Настройка переменных окружения
echo.
echo 3. Настройка переменных окружения...
.\nssm.exe set SCADA_Collector AppEnvironmentExtra "PATH=%PATH%;%PYTHON_DIR%;%PYTHON_DIR%Scripts" "PYTHONPATH=%SYSTEM_SITE%;%USER_SITE%"

REM Проверка настроек
echo.
echo 4. Проверка настроек...
echo Пользователь службы:
.\nssm.exe get SCADA_Collector ObjectName
echo.
echo Переменные окружения:
.\nssm.exe get SCADA_Collector AppEnvironmentExtra

REM Запуск службы
echo.
echo 5. Запуск службы...
.\nssm.exe start SCADA_Collector
timeout /t 5 /nobreak >nul

echo.
echo 6. Проверка статуса...
.\nssm.exe status SCADA_Collector

echo.
echo 7. Поиск процессов Python...
tasklist | findstr /i python

echo.
echo 8. Проверка доступа к сетевой папке...
echo Тестирование подключения к \\192.168.230.241\c$\hscmt\Ozbekiston\cal\H
if exist "\\192.168.230.241\c$\hscmt\Ozbekiston\cal\H" (
    echo [OK] Сетевая папка доступна!
    dir "\\192.168.230.241\c$\hscmt\Ozbekiston\cal\H" | findstr /i "TC-2" | findstr /c:"файлов"
) else (
    echo [WARNING] Сетевая папка недоступна. Проверьте подключение к сети.
)

echo.
echo 9. Проверка логов (последние 10 строк stderr)...
if exist service_stderr.log (
    powershell -Command "Get-Content service_stderr.log -Tail 10"
) else (
    echo [INFO] service_stderr.log не найден
)

echo.
echo ========================================
echo Готово!
echo ========================================
echo.
echo Служба настроена для запуска от имени: %SERVICE_USER%
echo Теперь служба будет использовать учетные данные этого пользователя
echo для доступа к сетевым ресурсам.
echo.
echo Примечание: Если служба не может подключиться к сетевой папке,
echo убедитесь, что пользователь %SERVICE_USER% имеет права доступа к папке.
echo.
pause


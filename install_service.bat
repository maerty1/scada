@echo off
chcp 1251
REM Установка службы SCADA Collector через NSSM
REM Запуск: install_service.bat

echo ========================================
echo Установка службы SCADA Collector
echo ========================================
echo.

REM Проверка наличия nssm.exe
if not exist "nssm.exe" (
    echo [ERROR] Файл nssm.exe не найден в текущей директории!
    echo Убедитесь, что nssm.exe находится в той же папке, что и этот скрипт.
    pause
    exit /b 1
)

REM Получаем текущую директорию
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Находим Python
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python не найден в PATH!
    echo Убедитесь, что Python установлен и добавлен в PATH.
    pause
    exit /b 1
)

REM Получаем полный путь к Python
for /f "delims=" %%i in ('where python') do set PYTHON_PATH=%%i

echo Текущая директория: %SCRIPT_DIR%
echo Python: %PYTHON_PATH%
echo.

REM Проверяем, существует ли служба
nssm.exe status SCADA_Collector >nul 2>&1
if %errorlevel% equ 0 (
    echo [WARNING] Служба SCADA_Collector уже установлена!
    echo.
    set /p CONFIRM="Переустановить службу? (Y/N): "
    if /i not "%CONFIRM%"=="Y" (
        echo Установка отменена.
        pause
        exit /b 0
    )
    echo Удаление существующей службы...
    nssm.exe stop SCADA_Collector
    timeout /t 2 /nobreak >nul
    nssm.exe remove SCADA_Collector confirm
    timeout /t 2 /nobreak >nul
    echo.
)

echo Установка службы...
echo.

REM Установка службы
nssm.exe install SCADA_Collector "%PYTHON_PATH%" "%SCRIPT_DIR%\collector.py"

if %errorlevel% neq 0 (
    echo [ERROR] Ошибка установки службы!
    pause
    exit /b 1
)

REM Настройка параметров службы
echo Настройка параметров службы...
echo.

REM Рабочая директория
nssm.exe set SCADA_Collector AppDirectory "%SCRIPT_DIR%"

REM Описание службы
nssm.exe set SCADA_Collector Description "SCADA Collector - Сбор и синхронизация данных SCADA систем"

REM Тип запуска - автоматический
nssm.exe set SCADA_Collector Start SERVICE_AUTO_START

REM Действия при сбое
nssm.exe set SCADA_Collector AppExit Default Restart
nssm.exe set SCADA_Collector AppRestartDelay 5000

REM Логирование
nssm.exe set SCADA_Collector AppStdout "%SCRIPT_DIR%\service_stdout.log"
nssm.exe set SCADA_Collector AppStderr "%SCRIPT_DIR%\service_stderr.log"
nssm.exe set SCADA_Collector AppRotateFiles 1
nssm.exe set SCADA_Collector AppRotateOnline 1
nssm.exe set SCADA_Collector AppRotateSeconds 86400
nssm.exe set SCADA_Collector AppRotateBytes 10485760

REM Переменные окружения (если нужны)
REM nssm.exe set SCADA_Collector AppEnvironmentExtra "PATH=%PATH%"

REM Приоритет (опционально)
REM nssm.exe set SCADA_Collector AppPriority NORMAL_PRIORITY_CLASS

echo.
echo ========================================
echo Служба успешно установлена!
echo ========================================
echo.
echo Имя службы: SCADA_Collector
echo Статус: Остановлена (запустите вручную или перезагрузите компьютер)
echo.
echo Команды для управления службой:
echo   Запуск:   nssm.exe start SCADA_Collector
echo   Остановка: nssm.exe stop SCADA_Collector
echo   Статус:   nssm.exe status SCADA_Collector
echo   Удаление: uninstall_service.bat
echo.
echo Или используйте стандартные команды Windows:
echo   net start SCADA_Collector
echo   net stop SCADA_Collector
echo   sc query SCADA_Collector
echo.

set /p START_NOW="Запустить службу сейчас? (Y/N): "
if /i "%START_NOW%"=="Y" (
    echo Запуск службы...
    nssm.exe start SCADA_Collector
    if %errorlevel% equ 0 (
        echo [OK] Служба запущена!
    ) else (
        echo [WARNING] Не удалось запустить службу. Проверьте логи.
    )
)

echo.
pause


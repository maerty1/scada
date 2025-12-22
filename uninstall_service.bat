@echo off
chcp 1251
REM Удаление службы SCADA Collector
REM Запуск: uninstall_service.bat

echo ========================================
echo Удаление службы SCADA Collector
echo ========================================
echo.

REM Проверка наличия nssm.exe
if not exist "nssm.exe" (
    echo [ERROR] Файл nssm.exe не найден в текущей директории!
    pause
    exit /b 1
)

REM Проверяем, существует ли служба
nssm.exe status SCADA_Collector >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Служба SCADA_Collector не установлена.
    pause
    exit /b 0
)

echo [WARNING] Будет удалена служба SCADA_Collector
echo.
set /p CONFIRM="Продолжить? (Y/N): "
if /i not "%CONFIRM%"=="Y" (
    echo Удаление отменено.
    pause
    exit /b 0
)

echo.
echo Остановка службы...
nssm.exe stop SCADA_Collector
timeout /t 3 /nobreak >nul

echo Удаление службы...
nssm.exe remove SCADA_Collector confirm

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo Служба успешно удалена!
    echo ========================================
) else (
    echo.
    echo [ERROR] Ошибка при удалении службы!
    echo Возможно, требуется запуск от имени администратора.
)

echo.
pause


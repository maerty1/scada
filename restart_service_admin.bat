@echo off
chcp 1251
REM Этот скрипт должен запускаться от имени администратора
REM Правый клик -> Запуск от имени администратора

echo ========================================
echo Перезапуск службы SCADA_Collector
echo ========================================
echo.

echo Остановка службы...
.\nssm.exe stop SCADA_Collector
timeout /t 3 /nobreak >nul

echo Запуск службы...
.\nssm.exe start SCADA_Collector
timeout /t 3 /nobreak >nul

echo.
echo Статус службы:
.\nssm.exe status SCADA_Collector

echo.
echo Процессы Python:
tasklist | findstr /i python

echo.
echo Проверка логов (последние 5 строк stderr):
if exist service_stderr.log (
    powershell -Command "Get-Content service_stderr.log -Tail 5"
) else (
    echo Лог не найден
)

echo.
pause


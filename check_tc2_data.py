#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Проверка данных TC2 в файле и БД"""
import pandas as pd
from pathlib import Path
import pyodbc
from datetime import datetime

# Проверка файла
file_path = Path(r'\\192.168.230.241\c$\hscmt\Ozbekiston\cal\H\2025-12-23_TC-2.xlsx')
print("=" * 60)
print("Проверка файла TC2")
print("=" * 60)

if file_path.exists():
    try:
        df = pd.read_excel(file_path, skipfooter=1)
        print(f"Файл найден: {file_path}")
        print(f"Колонки: {[str(c) for c in df.columns]}")
        print(f"Всего строк: {len(df)}")
        
        # Ищем колонку с датой
        date_col = None
        for col in df.columns:
            if 'дата' in col.lower() or 'время' in col.lower():
                date_col = col
                break
        
        if date_col:
            df['check_datetime'] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=['check_datetime'])
            print(f"\nКолонка с датой: {date_col}")
            print(f"Записей с датой: {len(df)}")
            if len(df) > 0:
                print(f"Диапазон дат в файле: {df['check_datetime'].min()} - {df['check_datetime'].max()}")
                print(f"\nПоследние 5 записей:")
                print(df[['check_datetime']].tail())
        else:
            print("Колонка с датой не найдена!")
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")
else:
    print(f"Файл не найден: {file_path}")

# Проверка БД
print("\n" + "=" * 60)
print("Проверка БД")
print("=" * 60)

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=BlueStarDB;'
        'UID=sa;'
        'PWD=01q335LA'
    )
    cursor = conn.cursor()
    
    # Получаем структуру таблицы
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Dynamic_TC2' 
        ORDER BY ORDINAL_POSITION
    """)
    columns = cursor.fetchall()
    print("Колонки в таблице Dynamic_TC2:")
    for col in columns:
        print(f"  {col[0]} ({col[1]})")
    
    # Ищем колонку с датой
    date_col_db = None
    for col in columns:
        if 'date' in col[0].lower() or 'time' in col[0].lower() or col[0] == 'check_datetime':
            date_col_db = col[0]
            break
    
    if date_col_db:
        cursor.execute(f"SELECT TOP 5 {date_col_db} FROM dbo.Dynamic_TC2 ORDER BY {date_col_db} DESC")
        rows = cursor.fetchall()
        print(f"\nПоследние 5 записей в БД (колонка {date_col_db}):")
        for row in rows:
            print(f"  {row[0]}")
        
        cursor.execute(f"SELECT MAX({date_col_db}) FROM dbo.Dynamic_TC2")
        max_time = cursor.fetchone()[0]
        print(f"\nМаксимальное время в БД: {max_time}")
    else:
        print("Колонка с датой не найдена в таблице!")
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dynamic_TC2")
    count = cursor.fetchone()[0]
    print(f"\nВсего записей в таблице: {count}")
    
    conn.close()
except Exception as e:
    print(f"Ошибка подключения к БД: {e}")


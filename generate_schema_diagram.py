"""
Скрипт для генерации PDF схемы архитектуры системы сбора данных SCADA
Визуальная диаграмма с блоками и стрелками
"""
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import json
from datetime import datetime
import os

# Загрузка конфигурации
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
except:
    config = {}

# Загрузка шрифта для кириллицы
FONT_NAME = 'Helvetica'  # По умолчанию
FONT_BOLD = 'Helvetica-Bold'

# Пробуем загрузить Arial Unicode MS (лучшая поддержка кириллицы)
arial_paths = [
    'C:/Windows/Fonts/arialuni.ttf',  # Arial Unicode MS
    'C:/Windows/Fonts/arial.ttf',      # Arial
    'C:/Windows/Fonts/tahoma.ttf',     # Tahoma
    'C:/Windows/Fonts/calibri.ttf',    # Calibri
]

for font_path in arial_paths:
    if os.path.exists(font_path):
        try:
            font_name = os.path.basename(font_path).replace('.ttf', '').replace('.TTF', '')
            pdfmetrics.registerFont(TTFont(font_name, font_path))
            FONT_NAME = font_name
            # Пробуем найти bold версию
            bold_path = font_path.replace('.ttf', 'bd.ttf').replace('.TTF', 'BD.TTF')
            if not os.path.exists(bold_path):
                bold_path = font_path.replace('arial.ttf', 'arialbd.ttf').replace('tahoma.ttf', 'tahomabd.ttf')
            if os.path.exists(bold_path):
                pdfmetrics.registerFont(TTFont(font_name + '-Bold', bold_path))
                FONT_BOLD = font_name + '-Bold'
            print(f"Загружен шрифт: {font_name} из {font_path}")
            break
        except Exception as e:
            print(f"Ошибка загрузки шрифта {font_path}: {e}")
            continue

def draw_box(c, x, y, width, height, text, color=colors.lightblue, text_color=colors.black, font_size=9):
    """Рисует прямоугольник с текстом"""
    # Рамка
    c.setStrokeColor(color)
    c.setFillColor(color)
    c.rect(x, y, width, height, fill=1, stroke=1)
    
    # Текст
    c.setFillColor(text_color)
    c.setFont(FONT_NAME, font_size)
    
    # Разбиваем текст на строки
    words = text.split('\n')
    line_height = font_size + 2
    start_y = y + height - (height - len(words) * line_height) / 2 - line_height
    
    for i, word in enumerate(words):
        text_width = c.stringWidth(word, FONT_NAME, font_size)
        text_x = x + (width - text_width) / 2
        c.drawString(text_x, start_y - i * line_height, word)

def draw_arrow(c, x1, y1, x2, y2, color=colors.black):
    """Рисует стрелку от (x1, y1) к (x2, y2)"""
    c.setStrokeColor(color)
    c.setLineWidth(1.5)
    
    # Линия
    c.line(x1, y1, x2, y2)
    
    # Стрелка
    import math
    angle = math.atan2(y2 - y1, x2 - x1)
    arrow_length = 8
    arrow_angle = 0.5
    
    # Координаты стрелки
    x3 = x2 - arrow_length * math.cos(angle - arrow_angle)
    y3 = y2 - arrow_length * math.sin(angle - arrow_angle)
    x4 = x2 - arrow_length * math.cos(angle + arrow_angle)
    y4 = y2 - arrow_length * math.sin(angle + arrow_angle)
    
    c.line(x2, y2, x3, y3)
    c.line(x2, y2, x4, y4)

def create_architecture_diagram():
    """Создание PDF документа со схемой архитектуры"""
    
    filename = 'SCADA_Collector_Architecture.pdf'
    c = canvas.Canvas(filename, pagesize=landscape(A4))
    width, height = landscape(A4)
    
    # Заголовок
    c.setFont(FONT_BOLD, 20)
    c.setFillColor(colors.HexColor('#1a1a1a'))
    title = "Архитектура системы сбора и хранения данных SCADA"
    title_width = c.stringWidth(title, FONT_NAME, 20)
    c.drawString((width - title_width) / 2, height - 2*cm, title)
    
    # Дата
    c.setFont(FONT_NAME, 10)
    date_text = f"Дата создания: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    c.drawString(2*cm, height - 2.5*cm, date_text)
    
    # Координаты для схемы
    margin_x = 2*cm
    margin_y = 3*cm
    box_width = 4*cm
    box_height = 1.5*cm
    spacing_x = 1*cm
    spacing_y = 1.5*cm
    
    # === ИСТОЧНИКИ ДАННЫХ (слева) ===
    sources_y = height - 5*cm
    
    # MSSQL источники
    mssql_count = len(config.get('sync_mssql', []))
    draw_box(c, margin_x, sources_y, box_width, box_height, 
             f"MSSQL\nИсточники\n({mssql_count} таблиц)", 
             colors.HexColor('#27ae60'), colors.white, 10)
    
    # Firebird источники
    firebird_count = len(config.get('sync_firebird', []))
    draw_box(c, margin_x, sources_y - box_height - spacing_y, box_width, box_height,
             f"Firebird\nИсточники\n({firebird_count} источников)",
             colors.HexColor('#e74c3c'), colors.white, 10)
    
    # TC2 Excel файлы
    tc2_config = config.get('tc2_processor', {})
    if tc2_config.get('enabled', False):
        draw_box(c, margin_x, sources_y - 2*(box_height + spacing_y), box_width, box_height,
                 "TC2 Excel\nФайлы\n(Сетевая папка)",
                 colors.HexColor('#f39c12'), colors.white, 10)
    
    # === COLLECTOR (центр) ===
    collector_x = margin_x + box_width + 3*cm
    collector_y = height - 6*cm
    
    draw_box(c, collector_x, collector_y, box_width + 1*cm, box_height * 2,
             "SCADA Collector\n(Python asyncio)\n\n• MSSQL Sync\n• Firebird Sync\n• TC2 Processor\n• Monitoring",
             colors.HexColor('#3498db'), colors.white, 10)
    
    # === ЦЕЛЕВАЯ БД (справа) ===
    target_x = collector_x + box_width + 1*cm + 3*cm
    target_y = height - 6*cm
    
    db_name = config.get('database', {}).get('database', 'BlueStarDB')
    table_count = len(config.get('table_names', {}))
    draw_box(c, target_x, target_y, box_width + 1*cm, box_height * 2,
             f"MSSQL Server\n{db_name}\n\nТаблицы Dynamic_*\n({table_count} таблиц)",
             colors.HexColor('#9b59b6'), colors.white, 10)
    
    # === СТРЕЛКИ ===
    # От источников к Collector
    arrow_start_x = margin_x + box_width
    arrow_end_x = collector_x
    
    # MSSQL -> Collector
    draw_arrow(c, arrow_start_x, sources_y + box_height/2, arrow_end_x, collector_y + box_height * 1.5, colors.HexColor('#27ae60'))
    
    # Firebird -> Collector
    draw_arrow(c, arrow_start_x, sources_y - box_height/2 - spacing_y, arrow_end_x, collector_y + box_height, colors.HexColor('#e74c3c'))
    
    # TC2 -> Collector
    if tc2_config.get('enabled', False):
        draw_arrow(c, arrow_start_x, sources_y - box_height*1.5 - 2*spacing_y, arrow_end_x, collector_y + box_height/2, colors.HexColor('#f39c12'))
    
    # Collector -> Target DB
    arrow_start_x = collector_x + box_width + 1*cm
    arrow_end_x = target_x
    draw_arrow(c, arrow_start_x, collector_y + box_height, arrow_end_x, target_y + box_height, colors.HexColor('#9b59b6'))
    
    # === WEB И TELEGRAM (внизу) ===
    web_y = margin_y + 2*cm
    
    # Web интерфейс
    web_config = config.get('web', {})
    web_port = web_config.get('port', 80)
    draw_box(c, collector_x, web_y, box_width, box_height,
             f"Web Interface\nFlask\nPort: {web_port}",
             colors.HexColor('#16a085'), colors.white, 9)
    
    # Telegram
    telegram_config = config.get('telegram', {})
    draw_box(c, collector_x + box_width + spacing_x, web_y, box_width, box_height,
             "Telegram\nNotifications\nBot API",
             colors.HexColor('#16a085'), colors.white, 9)
    
    # Стрелки от Collector к Web/Telegram
    arrow_start_y = collector_y
    arrow_end_y = web_y + box_height
    draw_arrow(c, collector_x + (box_width + 1*cm)/2, arrow_start_y, collector_x + box_width/2, arrow_end_y, colors.HexColor('#16a085'))
    draw_arrow(c, collector_x + (box_width + 1*cm)/2, arrow_start_y, collector_x + box_width + spacing_x + box_width/2, arrow_end_y, colors.HexColor('#16a085'))
    
    # === ЛЕГЕНДА ===
    legend_x = margin_x
    legend_y = margin_y - 0.5*cm
    
    c.setFont(FONT_NAME, 9)
    c.setFillColor(colors.black)
    c.drawString(legend_x, legend_y, "Легенда:")
    
    legend_y -= 0.4*cm
    box_size = 0.3*cm
    
    # MSSQL
    c.setFillColor(colors.HexColor('#27ae60'))
    c.rect(legend_x, legend_y - box_size/2, box_size, box_size, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + box_size + 0.2*cm, legend_y - 0.1*cm, "MSSQL источники")
    
    # Firebird
    legend_x += 3*cm
    c.setFillColor(colors.HexColor('#e74c3c'))
    c.rect(legend_x, legend_y - box_size/2, box_size, box_size, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + box_size + 0.2*cm, legend_y - 0.1*cm, "Firebird источники")
    
    # TC2
    legend_x += 3*cm
    c.setFillColor(colors.HexColor('#f39c12'))
    c.rect(legend_x, legend_y - box_size/2, box_size, box_size, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + box_size + 0.2*cm, legend_y - 0.1*cm, "TC2 Excel")
    
    # Collector
    legend_x += 3*cm
    c.setFillColor(colors.HexColor('#3498db'))
    c.rect(legend_x, legend_y - box_size/2, box_size, box_size, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + box_size + 0.2*cm, legend_y - 0.1*cm, "Collector")
    
    # Target DB
    legend_x += 3*cm
    c.setFillColor(colors.HexColor('#9b59b6'))
    c.rect(legend_x, legend_y - box_size/2, box_size, box_size, fill=1)
    c.setFillColor(colors.black)
    c.drawString(legend_x + box_size + 0.2*cm, legend_y - 0.1*cm, "Целевая БД")
    
    # === ДЕТАЛИ НА ВТОРОЙ СТРАНИЦЕ ===
    c.showPage()
    
    # Заголовок второй страницы
    c.setFont(FONT_NAME, 16)
    c.setFillColor(colors.HexColor('#1a1a1a'))
    detail_title = "Детали конфигурации"
    title_width = c.stringWidth(detail_title, FONT_NAME, 16)
    c.drawString((width - title_width) / 2, height - 2*cm, detail_title)
    
    # Детали источников
    y_pos = height - 4*cm
    c.setFont(FONT_NAME, 12)
    c.setFillColor(colors.HexColor('#27ae60'))
    c.drawString(margin_x, y_pos, "MSSQL Источники:")
    
    y_pos -= 0.6*cm
    c.setFont(FONT_NAME, 9)
    c.setFillColor(colors.black)
    
    mssql_sources = config.get('sync_mssql', [])
    source_server = mssql_sources[0].get('source_server', 'N/A') if mssql_sources else 'N/A'
    c.drawString(margin_x + 0.5*cm, y_pos, f"Сервер: {source_server}")
    y_pos -= 0.4*cm
    c.drawString(margin_x + 0.5*cm, y_pos, f"База данных: {mssql_sources[0].get('source_db', 'N/A') if mssql_sources else 'N/A'}")
    y_pos -= 0.4*cm
    c.drawString(margin_x + 0.5*cm, y_pos, f"Всего таблиц: {len(mssql_sources)}")
    
    # Примеры таблиц
    y_pos -= 0.6*cm
    c.setFont(FONT_NAME, 8)
    table_examples = []
    for source in mssql_sources[:5]:
        table_examples.append(f"{source.get('source_table', '')} -> {source.get('target_table', '')}")
    
    for example in table_examples:
        y_pos -= 0.35*cm
        c.drawString(margin_x + 1*cm, y_pos, f"• {example}")
    
    if len(mssql_sources) > 5:
        y_pos -= 0.35*cm
        c.drawString(margin_x + 1*cm, y_pos, f"• ... и еще {len(mssql_sources) - 5} таблиц")
    
    # Firebird источники
    y_pos -= 0.8*cm
    c.setFont(FONT_NAME, 12)
    c.setFillColor(colors.HexColor('#e74c3c'))
    c.drawString(margin_x, y_pos, "Firebird Источники:")
    
    y_pos -= 0.6*cm
    c.setFont(FONT_NAME, 9)
    c.setFillColor(colors.black)
    
    firebird_sources = config.get('sync_firebird', [])
    for fb_source in firebird_sources:
        host = fb_source.get('host', 'N/A')
        target_table = fb_source.get('target_table', 'N/A')
        objid = fb_source.get('objid', 'N/A')
        table_name = config.get('table_names', {}).get(target_table, target_table)
        y_pos -= 0.4*cm
        c.drawString(margin_x + 0.5*cm, y_pos, f"• {host} -> {target_table} (OBJID={objid}) - {table_name}")
    
    # TC2
    if tc2_config.get('enabled', False):
        y_pos -= 0.8*cm
        c.setFont(FONT_NAME, 12)
        c.setFillColor(colors.HexColor('#f39c12'))
        c.drawString(margin_x, y_pos, "TC2 Процессор:")
        
        y_pos -= 0.6*cm
        c.setFont(FONT_NAME, 9)
        c.setFillColor(colors.black)
        c.drawString(margin_x + 0.5*cm, y_pos, f"Директория: {tc2_config.get('files_directory', 'N/A')}")
        y_pos -= 0.4*cm
        c.drawString(margin_x + 0.5*cm, y_pos, f"Интервал проверки: {tc2_config.get('file_check_interval', 3600) / 3600:.1f} часов")
        y_pos -= 0.4*cm
        c.drawString(margin_x + 0.5*cm, y_pos, f"Целевая таблица: {tc2_config.get('target_table', 'N/A')}")
    
    # Целевая БД
    y_pos -= 0.8*cm
    c.setFont(FONT_NAME, 12)
    c.setFillColor(colors.HexColor('#9b59b6'))
    c.drawString(margin_x, y_pos, "Целевая база данных:")
    
    y_pos -= 0.6*cm
    c.setFont(FONT_NAME, 9)
    c.setFillColor(colors.black)
    db_config = config.get('database', {})
    c.drawString(margin_x + 0.5*cm, y_pos, f"Сервер: {db_config.get('server', 'N/A')}")
    y_pos -= 0.4*cm
    c.drawString(margin_x + 0.5*cm, y_pos, f"База данных: {db_config.get('database', 'N/A')}")
    y_pos -= 0.4*cm
    c.drawString(margin_x + 0.5*cm, y_pos, f"Всего таблиц: {len(config.get('table_names', {}))}")
    
    # Интервалы синхронизации
    y_pos -= 0.8*cm
    c.setFont(FONT_NAME, 12)
    c.setFillColor(colors.HexColor('#34495e'))
    c.drawString(margin_x, y_pos, "Интервалы синхронизации:")
    
    y_pos -= 0.6*cm
    c.setFont(FONT_NAME, 9)
    c.setFillColor(colors.black)
    sync_interval = config.get('sync_interval', 5)
    c.drawString(margin_x + 0.5*cm, y_pos, f"MSSQL/Firebird: каждые {sync_interval} секунд")
    y_pos -= 0.4*cm
    if tc2_config.get('enabled', False):
        tc2_interval = tc2_config.get('file_check_interval', 3600) / 3600
        c.drawString(margin_x + 0.5*cm, y_pos, f"TC2: каждые {tc2_interval:.1f} часов")
        y_pos -= 0.4*cm
    notification_timeout = config.get('notification_timeout', 7200) / 3600
    c.drawString(margin_x + 0.5*cm, y_pos, f"Timeout уведомлений: {notification_timeout:.1f} часов")
    
    # Сохранение
    c.save()
    print(f"PDF схема создана: {filename}")
    return filename

if __name__ == "__main__":
    create_architecture_diagram()

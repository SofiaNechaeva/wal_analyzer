import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.io as pio
from matplotlib.backends.backend_pdf import PdfPages
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import json
import os
import matplotlib.dates as mdates

def get_arial_font_path():
    system = os.name
    
    if system == 'nt':  # Windows
        font_path = r"C:\Windows\Fonts\arial.ttf"
    elif system == 'posix':  # Linux, macOS
        # Попробуем несколько возможных путей в Linux
        possible_paths = [
            "/usr/share/fonts/truetype/msttcorefonts/arial.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/arial.ttf",
            "/usr/local/share/fonts/arial.ttf",
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                font_path = path
                break
        else:
            # Если не нашли Arial, используем системный шрифт sans-serif
            font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    else:
        raise OSError("Неподдерживаемая операционная система")
    
    return font_path

# Регистрация шрифта
try:
    font_path = get_arial_font_path()
    pdfmetrics.registerFont(TTFont("Arial", font_path))
except Exception as e:
    print(f"Не удалось зарегистрировать Arial: {e}")

class ReportBuilder:
    def __init__(self, slot_config: dict, db_path="wal_analyzer.db"):
        self.conn = sqlite3.connect(db_path)
        self.slot_name = slot_config['slot_name']
        self.plots = []         # matplotlib figures
        self.plotly_figs = []   # plotly figures

    def pie_operations(self):
        df = pd.read_sql_query("""
            SELECT operation, count FROM agg_operations
            WHERE slot_name = ?
        """, self.conn, params=(self.slot_name,))
        if df.empty:
            return
        fig, ax = plt.subplots()
        ax.pie(df['count'], labels=df['operation'], autopct='%1.1f%%', startangle=90)
        ax.set_title("Операции")
        self.plots.append(fig)

        fig_plotly = px.pie(df, names='operation', values='count', title='Операции')
        self.plotly_figs.append(fig_plotly)

    def activity_line(self):
        df = pd.read_sql_query("""
            SELECT bucket_start, count FROM agg_activity
            WHERE slot_name = ?
            ORDER BY bucket_start
        """, self.conn, params=(self.slot_name,))
        print(df.shape)
        if df.empty:
            # ничего не добавляем, просто выходим
            return

        df['time'] = pd.to_datetime(df['bucket_start'], unit='s', utc=True)
        df['time'] = df['time'].dt.tz_convert('Europe/Moscow')

        if len(df) == 1:
            # добавим вторую точку с нулевым значением чуть раньше
            earlier = df['time'].iloc[0] - pd.Timedelta(minutes=1)
            df = pd.concat([pd.DataFrame({'time': [earlier], 'count': [0]}), df])

        # Matplotlib версия (для PDF)
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(df['time'], df['count'], marker='o')
        ax.set_title("Активность по времени")
        ax.set_xlabel("Время")
        ax.set_ylabel("События")
        ax.tick_params(axis='x', labelrotation=45)
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m %H:%M'))
        self.plots.append(fig)

        # Plotly версия (для HTML)
        fig_plotly = px.line(
            df, x='time', y='count',
            title='Активность по времени',
            markers=True
        )
        fig_plotly.update_layout(
            xaxis=dict(
                tickformat="%d.%m %H:%M",
                tickangle=45,
                nticks=6
            )
        )
        self.plotly_figs.append(fig_plotly)

    def heatmap_tables(self):
        df = pd.read_sql_query("""
            SELECT schema, table_name, count FROM agg_tables
            WHERE slot_name = ?
        """, self.conn, params=(self.slot_name,))
        if df.empty:
            # ничего не добавляем, просто выходим
            return
        pivot = df.pivot(index='schema', columns='table_name', values='count').fillna(0)
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="YlGnBu", ax=ax)
        ax.set_title("Тепловая карта по таблицам")
        self.plots.append(fig)

        fig_plotly = px.imshow(pivot, text_auto=True, aspect="auto", title="Тепловая карта по таблицам")
        self.plotly_figs.append(fig_plotly)

    def size_histogram(self):
        df = pd.read_sql_query("""
            SELECT size_bucket, count FROM agg_sizes
            WHERE slot_name = ?
        """, self.conn, params=(self.slot_name,))
        if df.empty:
            # ничего не добавляем, просто выходим
            return
        fig, ax = plt.subplots()
        ax.bar(df['size_bucket'], df['count'], color='skyblue')
        ax.set_title("Размеры событий")
        ax.set_xlabel("Размер")
        ax.set_ylabel("Количество")
        self.plots.append(fig)

        fig_plotly = px.bar(df, x='size_bucket', y='count', title='Размеры событий')
        self.plotly_figs.append(fig_plotly)

    def save_pdf(self, filename="report.pdf"):
        with PdfPages(filename) as pdf:
            if not self.plots:  # если нет ни одной фигуры
                fig, ax = plt.subplots()
                ax.text(0.5, 0.5, "Изменений, соответствующих фильтрам, не было",
                        ha='center', va='center', fontsize=12)
                ax.axis("off")
                ax.set_title("Отчёт")
                pdf.savefig(fig)
                plt.close(fig)
            else:
                for fig in self.plots:
                    pdf.savefig(fig)
                    plt.close(fig)


    def save_html(self, filename="report.html"):
        html_parts = []
        if not self.plotly_figs:  # если нет ни одной фигуры
            html_parts.append("<div style='text-align:center; font-size:16px;'>"
                            "Изменений, соответствующих фильтрам, не было</div>")
        else:
            for fig in self.plotly_figs:
                html_parts.append(pio.to_html(fig, full_html=False, include_plotlyjs='cdn'))

        html_str = ("<html><head><meta charset='utf-8'></head><body>"
                    + "\n".join(html_parts) +
                    "</body></html>")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_str)


    def mask_fields(self, data: dict, masks_fields: list) -> dict:
        """
        Маскирует значения словаря по правилам:
        - Заглавные буквы и цифры → '#'
        - Строчные буквы → '*'
        - Знаки и служебные символы остаются как есть
        """
        def mask_value(val: str) -> str:
            if not isinstance(val, str):
                return val
            masked = []
            for ch in val:
                if ch.isupper() or ch.isdigit():
                    masked.append('#')
                elif ch.islower():
                    masked.append('*')
                else:
                    masked.append(ch)
            return "".join(masked)

        return {
            k: mask_value(v) if k in masks_fields else v
            for k, v in data.items()
        }

    def aggregate_jsonl_to_pdfs(self, jsonl_path: str, slot_name: str, table: str,
                            ids: list, output_dir: str, columns: list, masks_fields: list):
        # создаём canvas для каждого Id
        canvases = {}
        positions = {}

        for id_value in ids:
            filename = f"{slot_name}_{table}_{id_value}.pdf"
            filepath = os.path.join(output_dir, filename)

            c = canvas.Canvas(filepath, pagesize=A4)
            c.setFont("Arial", 12)  # стандартный шрифт, точно работает
            width, height = A4
            y = height - 50

            if os.path.exists(filepath):
                c.drawString(50, y, f"Дополнение к истории Id={id_value}, slot={slot_name}")
                y -= 30
            else:
                c.drawString(50, y, f"История изменений Id={id_value}, slot={slot_name}")
                y -= 30

            canvases[id_value] = (c, width, height)
            positions[id_value] = y

        # читаем JSONL построчно один раз
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except Exception:
                    continue

                if ev.get("table") != table:
                    continue

                old_data_list = ev.get("old_data") or []
                new_data_list = ev.get("new_data") or []
                # превращаем списки в словари
                old_data = dict(zip(columns, old_data_list)) if old_data_list else {}
                new_data = dict(zip(columns, new_data_list)) if new_data_list else {}

                # маскируем
                old_data = self.mask_fields(old_data, masks_fields)
                new_data = self.mask_fields(new_data, masks_fields)
                print(new_data)               
                # проверяем, есть ли id в данных
                for id_value in ids:
                    old_id = old_data.get("id")
                    new_id = new_data.get("id")
                    

                    match = (str(id_value) == str(old_id)) or (str(id_value) == str(new_id))
                    print(match)
                    if match:
                        c, width, height = canvases[id_value]
                        y = positions[id_value]

                        line_text = f"{old_data} -> {new_data}"
                        c.drawString(50, y, line_text)
                        y -= 20
                        if y < 50:
                            c.showPage()
                            y = height - 50

                        positions[id_value] = y

                        # сохраняем все PDF
        for id_value, (c, _, _) in canvases.items():
            c.save()

        return [os.path.join(output_dir, f"{slot_name}_{table}_{id_value}.pdf") for id_value in ids]

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

pdfmetrics.registerFont(TTFont('DejaVuSans', 'DejaVuSans.ttf'))

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
        df['time'] = pd.to_datetime(df['bucket_start'], unit='s')
        fig, ax = plt.subplots()
        ax.plot(df['time'], df['count'])
        ax.set_title("Активность по времени")
        ax.set_xlabel("Время")
        ax.set_ylabel("События")
        self.plots.append(fig)

        fig_plotly = px.line(df, x='time', y='count', title='Активность по времени')
        self.plotly_figs.append(fig_plotly)

    def heatmap_tables(self):
        df = pd.read_sql_query("""
            SELECT schema, table_name, count FROM agg_tables
            WHERE slot_name = ?
        """, self.conn, params=(self.slot_name,))
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
            for fig in self.plots:
                pdf.savefig(fig)
                plt.close(fig)

    def save_html(self, filename="report.html"):
        # Собираем все фигуры в один HTML
        html_parts = []
        for fig in self.plotly_figs:
            html_parts.append(pio.to_html(fig, full_html=False, include_plotlyjs='cdn'))
        # Оборачиваем в единый HTML
        html_str = "<html><head><meta charset='utf-8'></head><body>" + "\n".join(html_parts) + "</body></html>"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_str)

    def aggregate_jsonl_to_pdfs(self, jsonl_path: str, slot_name: str, table: str, ids: list, output_dir: str):
        # создаём canvas для каждого Id
        canvases = {}
        positions = {}

        for id_value in ids:
            filename = f"{slot_name}_{table}_{id_value}.pdf"
            filepath = os.path.join(output_dir, filename)

            c = canvas.Canvas(filepath, pagesize=A4)
            c.setFont("DejaVuSans", 12)
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

        # читаем JSONL построчно
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except Exception:
                    continue

                if ev.get("table") != table:
                    continue

                old_data = ev.get("old_data") or {}
                new_data = ev.get("new_data") or {}

                for id_value in ids:
                    if str(id_value) in json.dumps(old_data) or str(id_value) in json.dumps(new_data):
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
        return filepath
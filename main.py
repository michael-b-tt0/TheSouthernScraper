"""
Southern Railway Price Scraper — PyQt6 GUI
Run:  ./SouthernVe/Scripts/python.exe main.py
"""

import sys
import datetime

from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont, QColor, QPalette, QIcon, QTextCharFormat
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QDateEdit, QComboBox, QPushButton, QTextEdit, QCalendarWidget,
    QSplitter, QFrame, QGroupBox, QSizePolicy, QFileDialog,
)

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from scraper import ScraperThread


# ── Colour palette ──────────────────────────────────────────────────
BG_DARK      = "#0f1117"
BG_CARD      = "#1a1d2e"
BG_INPUT     = "#252840"
ACCENT       = "#6c63ff"
ACCENT_HOVER = "#8b83ff"
TEXT         = "#e8e8f0"
TEXT_DIM     = "#8888a0"
BORDER       = "#2e3150"
SUCCESS      = "#42d392"
DANGER       = "#ff6b6b"
CHART_LINE   = "#6c63ff"
CHART_DOT    = "#42d392"


STYLESHEET = f"""
QMainWindow, QWidget {{
    background-color: {BG_DARK};
    color: {TEXT};
    font-family: 'Segoe UI', 'Inter', sans-serif;
    font-size: 13px;
}}
QGroupBox {{
    background-color: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    margin-top: 14px;
    padding: 18px 14px 14px 14px;
    font-weight: 600;
    font-size: 14px;
    color: {TEXT};
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    left: 16px;
    padding: 0 6px;
    color: {ACCENT};
}}
QLabel {{
    color: {TEXT_DIM};
    font-size: 12px;
}}
QLineEdit, QDateEdit, QComboBox {{
    background-color: {BG_INPUT};
    border: 1px solid {BORDER};
    border-radius: 6px;
    padding: 8px 12px;
    color: {TEXT};
    font-size: 13px;
    selection-background-color: {ACCENT};
}}
QLineEdit:focus, QDateEdit:focus, QComboBox:focus {{
    border: 1px solid {ACCENT};
}}
QComboBox QAbstractItemView {{
    background-color: {BG_CARD};
    color: {TEXT};
    selection-background-color: {ACCENT};
    selection-color: #ffffff;
    border: 1px solid {BORDER};
}}
QPushButton#startBtn {{
    background-color: {ACCENT};
    color: #ffffff;
    border: none;
    border-radius: 8px;
    padding: 10px 28px;
    font-size: 14px;
    font-weight: 600;
    min-height: 20px;
}}
QPushButton#startBtn:hover {{
    background-color: {ACCENT_HOVER};
}}
QPushButton#startBtn:disabled {{
    background-color: {BG_INPUT};
    color: {TEXT_DIM};
}}
QPushButton#stopBtn {{
    background-color: {DANGER};
    color: #ffffff;
    border: none;
    border-radius: 8px;
    padding: 10px 28px;
    font-size: 14px;
    font-weight: 600;
    min-height: 20px;
}}
QPushButton#stopBtn:hover {{
    background-color: #ff8888;
}}
QTextEdit {{
    background-color: {BG_INPUT};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 10px;
    color: {TEXT};
    font-family: 'Cascadia Code', 'Consolas', monospace;
    font-size: 12px;
}}
QSplitter::handle {{
    background-color: {BORDER};
    height: 2px;
}}
QCalendarWidget QWidget {{
    alternate-background-color: {BG_CARD};
}}
QCalendarWidget QWidget#qt_calendar_navigationbar {{
    background-color: {BG_CARD};
    border-bottom: 1px solid {BORDER};
}}
QCalendarWidget QToolButton {{
    color: {TEXT};
    background-color: {BG_CARD};
    border: none;
    padding: 6px;
}}
QCalendarWidget QMenu {{
    background-color: {BG_CARD};
    color: {TEXT};
}}
QCalendarWidget QSpinBox {{
    color: {TEXT};
    background-color: {BG_CARD};
    selection-background-color: {ACCENT};
}}
QCalendarWidget QAbstractItemView:enabled {{
    color: {TEXT};
    background-color: {BG_CARD};
    selection-background-color: {ACCENT};
    selection-color: #ffffff;
}}
QCalendarWidget QHeaderView::section {{
    color: {TEXT};
    background-color: {BG_INPUT};
    padding: 4px 0;
    border: 0px;
}}
"""


class PriceChart(FigureCanvas):
    """Embedded matplotlib chart styled for the dark theme."""

    def __init__(self, parent=None):
        self.fig = Figure(facecolor=BG_CARD, edgecolor=BG_CARD)
        super().__init__(self.fig)
        self.ax = self.fig.add_subplot(111)
        self._style_axes()
        self.fig.tight_layout(pad=2.5)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def _style_axes(self):
        ax = self.ax
        ax.set_facecolor(BG_CARD)
        for spine in ax.spines.values():
            spine.set_color(BORDER)
        ax.tick_params(colors=TEXT_DIM, labelsize=9)
        ax.xaxis.label.set_color(TEXT_DIM)
        ax.yaxis.label.set_color(TEXT_DIM)
        ax.title.set_color(TEXT)
        ax.grid(True, color=BORDER, linewidth=0.5, alpha=0.6)

    def plot_prices(self, df):
        """Plot departure_dt vs price_gbp from the scraped DataFrame."""
        self.ax.clear()
        self._style_axes()

        # Drop rows with no price
        priced = df.dropna(subset=["price_gbp"]).copy()

        if priced.empty:
            self.ax.text(
                0.5, 0.5, "No priced trains found",
                transform=self.ax.transAxes, ha="center", va="center",
                fontsize=14, color=TEXT_DIM,
            )
            self.draw()
            return

        x = priced["departure_dt"]
        y = priced["price_gbp"]

        # Line + scatter
        self.ax.plot(x, y, color=CHART_LINE, linewidth=1.5, alpha=0.7, zorder=2)
        self.ax.scatter(x, y, color=CHART_DOT, s=36, edgecolors="white",
                        linewidths=0.5, zorder=3)

        # Fill under the line for a nice area effect
        self.ax.fill_between(x, y, alpha=0.08, color=CHART_LINE)

        # Annotate min / max
        min_idx = y.idxmin()
        max_idx = y.idxmax()
        for idx, label, colour in [(min_idx, "Low", SUCCESS), (max_idx, "High", DANGER)]:
            self.ax.annotate(
                f"£{y[idx]:.2f} ({label})",
                xy=(x[idx], y[idx]),
                xytext=(0, 14 if label == "High" else -18),
                textcoords="offset points",
                fontsize=9, fontweight="bold", color=colour,
                ha="center",
                arrowprops=dict(arrowstyle="-", color=colour, lw=0.8),
            )

        self.ax.set_xlabel("Departure time")
        self.ax.set_ylabel("Price (£)")
        self.ax.set_title("Southern Railway — Price Tracker", fontsize=14, fontweight="bold")

        # Format x-axis dates nicely
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b\n%H:%M"))
        self.fig.autofmt_xdate(rotation=0, ha="center")
        self.fig.tight_layout(pad=2.5)
        self.draw()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Southern Railway Price Scraper")
        self.setMinimumSize(900, 680)
        self.resize(1060, 780)
        self._thread = None
        self._df = None
        self._last_leaving = ""
        self._last_going = ""
        self._last_start_dt = None
        self._last_end_date = None

        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(18, 18, 18, 18)
        root_layout.setSpacing(12)

        # ── Header ──────────────────────────────────────────────────
        header = QLabel("🚆  Southern Railway Price Scraper")
        header.setStyleSheet(f"color: {TEXT}; font-size: 20px; font-weight: 700; padding: 4px 0;")
        header.setAlignment(Qt.AlignmentFlag.AlignLeft)
        root_layout.addWidget(header)

        # ── Input card ──────────────────────────────────────────────
        input_group = QGroupBox("Journey Details")
        input_layout = QHBoxLayout(input_group)
        input_layout.setSpacing(16)

        # Station from
        from_col = QVBoxLayout()
        from_col.addWidget(QLabel("LEAVING FROM"))
        self.from_edit = QLineEdit()
        self.from_edit.setPlaceholderText("e.g. Clapham Junction")
        from_col.addWidget(self.from_edit)
        input_layout.addLayout(from_col, 2)

        # Arrow indicator
        arrow = QLabel("→")
        arrow.setStyleSheet(f"color: {ACCENT}; font-size: 22px; font-weight: 700;")
        arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
        input_layout.addWidget(arrow)

        # Station to
        to_col = QVBoxLayout()
        to_col.addWidget(QLabel("GOING TO"))
        self.to_edit = QLineEdit()
        self.to_edit.setPlaceholderText("e.g. Brighton")
        to_col.addWidget(self.to_edit)
        input_layout.addLayout(to_col, 2)

        # Start date
        start_date_col = QVBoxLayout()
        start_date_col.addWidget(QLabel("START DATE"))
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("dd MMM yyyy")
        self._configure_calendar(self.start_date_edit)
        default_start = QDate.currentDate()
        self.start_date_edit.setDate(default_start)
        self.start_date_edit.setMinimumDate(QDate.currentDate())
        self.start_date_edit.dateChanged.connect(self._sync_date_constraints)
        start_date_col.addWidget(self.start_date_edit)
        input_layout.addLayout(start_date_col, 1)

        # Start time
        start_time_col = QVBoxLayout()
        start_time_col.addWidget(QLabel("START TIME"))
        self.start_time_combo = QComboBox()
        self.start_time_combo.addItems(self._build_time_options())
        start_time_col.addWidget(self.start_time_combo)
        input_layout.addLayout(start_time_col, 1)

        # End date
        date_col = QVBoxLayout()
        date_col.addWidget(QLabel("SCRAPE UNTIL"))
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDisplayFormat("dd MMM yyyy")
        self._configure_calendar(self.date_edit)
        default_end = QDate.currentDate().addDays(7)
        self.date_edit.setDate(default_end)
        self.date_edit.setMinimumDate(QDate.currentDate())
        date_col.addWidget(self.date_edit)
        input_layout.addLayout(date_col, 1)

        # Buttons
        btn_col = QVBoxLayout()
        btn_col.addWidget(QLabel(""))  # spacer label
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("▶  Start Scraping")
        self.start_btn.setObjectName("startBtn")
        self.start_btn.clicked.connect(self.on_start)
        btn_row.addWidget(self.start_btn)

        self.stop_btn = QPushButton("■  Stop")
        self.stop_btn.setObjectName("stopBtn")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.on_stop)
        btn_row.addWidget(self.stop_btn)
        
        self.export_btn = QPushButton("💾  Export CSV")
        self.export_btn.setObjectName("startBtn")
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.on_export)
        btn_row.addWidget(self.export_btn)
        
        btn_col.addLayout(btn_row)
        input_layout.addLayout(btn_col, 1)

        root_layout.addWidget(input_group)

        # ── Splitter: chart above, log below ────────────────────────
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Chart
        chart_group = QGroupBox("Price Overview")
        chart_layout = QVBoxLayout(chart_group)
        chart_layout.setContentsMargins(8, 16, 8, 8)
        self.chart = PriceChart()
        chart_layout.addWidget(self.chart)
        splitter.addWidget(chart_group)

        # Log
        log_group = QGroupBox("Scraper Log")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(8, 16, 8, 8)
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        log_layout.addWidget(self.log)
        splitter.addWidget(log_group)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        root_layout.addWidget(splitter, 1)

    # ── actions ─────────────────────────────────────────────────────
    def _configure_calendar(self, date_edit: QDateEdit):
        calendar = date_edit.calendarWidget()
        calendar.setHorizontalHeaderFormat(QCalendarWidget.HorizontalHeaderFormat.ShortDayNames)
        calendar.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
        calendar.setGridVisible(True)
        calendar.setMinimumSize(340, 240)
        weekday_format = QTextCharFormat()
        weekday_format.setForeground(QColor(TEXT))
        saturday_format = QTextCharFormat()
        saturday_format.setForeground(QColor(DANGER))
        sunday_format = QTextCharFormat()
        sunday_format.setForeground(QColor(DANGER))
        for day in (
            Qt.DayOfWeek.Monday,
            Qt.DayOfWeek.Tuesday,
            Qt.DayOfWeek.Wednesday,
            Qt.DayOfWeek.Thursday,
            Qt.DayOfWeek.Friday,
        ):
            calendar.setWeekdayTextFormat(day, weekday_format)
        calendar.setWeekdayTextFormat(Qt.DayOfWeek.Saturday, saturday_format)
        calendar.setWeekdayTextFormat(Qt.DayOfWeek.Sunday, sunday_format)

    def _build_time_options(self) -> list[str]:
        options = []
        for hour in range(24):
            for minute in (0, 15, 30, 45):
                options.append(f"{hour:02d}:{minute:02d}")
        return options

    def _sync_date_constraints(self):
        start_date = self.start_date_edit.date()
        self.date_edit.setMinimumDate(start_date)
        if self.date_edit.date() < start_date:
            self.date_edit.setDate(start_date)

    def on_start(self):
        leaving = self.from_edit.text().strip()
        going = self.to_edit.text().strip()
        if not leaving or not going:
            self.log.append("⚠  Please enter both stations.")
            return

        start_qdate = self.start_date_edit.date()
        start_time_text = self.start_time_combo.currentText()
        start_hour, start_minute = map(int, start_time_text.split(":"))
        start_dt = datetime.datetime(
            start_qdate.year(), start_qdate.month(), start_qdate.day(),
            start_hour, start_minute,
        )
        qdate = self.date_edit.date()
        end_date = datetime.date(qdate.year(), qdate.month(), qdate.day())
        if start_dt.date() > end_date:
            self.log.append("⚠  Start date must be on or before the scrape-until date.")
            return

        self.log.clear()
        start_label = start_dt.strftime("%Y-%m-%d %H:%M")
        self.log.append(f"Starting scrape: {leaving} → {going}  (from {start_label}, until {end_date})")

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.export_btn.setEnabled(False)

        self._last_leaving = leaving
        self._last_going = going
        self._last_start_dt = start_dt
        self._last_end_date = end_date

        self._thread = ScraperThread(leaving, going, start_qdate.toPyDate(), start_time_text, end_date)
        self._thread.progress.connect(self._on_progress)
        self._thread.results.connect(self._on_results)
        self._thread.error.connect(self._on_error)
        self._thread.finished.connect(self._on_finished)
        self._thread.start()

    def on_stop(self):
        if self._thread and self._thread.isRunning():
            self.log.append("Requesting stop …")
            self._thread.request_stop()

    def on_export(self):
        if self._df is None or self._df.empty:
            return
            
        # Build the dynamic default filename
        safe_from = self._last_leaving.replace(" ", "_").lower()
        safe_to = self._last_going.replace(" ", "_").lower()
        date_end = self._last_end_date.strftime("%Y-%m-%d") if self._last_end_date else "unknown"
        date_start = self._last_start_dt.strftime("%Y-%m-%d_%H%M") if self._last_start_dt else "unknown"
        default_name = f"{safe_from}_to_{safe_to}_{date_start}_until_{date_end}.csv"
        
        filename, _ = QFileDialog.getSaveFileName(
            self, "Export to CSV", default_name, "CSV Files (*.csv)"
        )
        if filename:
            try:
                self._df.to_csv(filename, index=False)
                self.log.append(f"\n✅  Exported {len(self._df)} rows to {filename}")
            except Exception as e:
                self.log.append(f"\n❌  Failed to export CSV: {e}")

    def _on_progress(self, msg: str):
        self.log.append(msg)

    def _on_results(self, df):
        self._df = df
        if not df.empty:
            self.export_btn.setEnabled(True)
        self.log.append(f"\n✅  Received {len(df)} train records.")
        if not df.empty:
            priced = df.dropna(subset=["price_gbp"])
            self.log.append(
                f"   Priced trains: {len(priced)}"
                f"   |  Min £{priced['price_gbp'].min():.2f}"
                f"   |  Max £{priced['price_gbp'].max():.2f}"
                f"   |  Avg £{priced['price_gbp'].mean():.2f}"
            )
        self.chart.plot_prices(df)

    def _on_error(self, msg: str):
        self.log.append(f"\n❌  Error: {msg}")

    def _on_finished(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._thread = None


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(STYLESHEET)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

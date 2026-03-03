"""
Southern Railway scraper — runs in a QThread so the GUI stays responsive.

Emits:
    progress(str)         – log messages for the GUI log panel
    results(pd.DataFrame) – the final scraped data
    error(str)            – fatal error description
"""

from PyQt6.QtCore import QThread, pyqtSignal
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import datetime
import random
import re
import time as t


class ScraperThread(QThread):
    progress = pyqtSignal(str)
    results = pyqtSignal(object)   # pd.DataFrame
    error = pyqtSignal(str)

    SITE_URL = "https://www.southernrailway.com"

    def __init__(self, leaving_from: str, going_to: str, end_date: datetime.date, parent=None):
        super().__init__(parent)
        self.leaving_from = leaving_from
        self.going_to = going_to
        self.end_date = end_date
        self._stop_flag = False

    # ------------------------------------------------------------------
    def request_stop(self):
        """Call from the main thread to ask the scraper to stop early."""
        self._stop_flag = True

    # ------------------------------------------------------------------
    def run(self):
        driver = None
        try:
            self.progress.emit("Launching browser …")
            driver = webdriver.Edge()
            driver.get(self.SITE_URL)
            t.sleep(5)

            # --- accept cookies ----
            self.progress.emit("Accepting cookies …")
            try:
                btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
                    )
                )
                btn.click()
                t.sleep(3)
            except TimeoutException:
                self.progress.emit("  Cookie banner not found – continuing anyway.")

            # --- fill station inputs (shadow DOM) ---
            self.progress.emit(f"Entering stations: {self.leaving_from} → {self.going_to}")
            shadow_host = driver.find_element(By.ID, "otrl-custom-hero")
            shadow_root = shadow_host.shadow_root

            leaving = shadow_root.find_element(By.NAME, "stationFrom")
            leaving.send_keys(self.leaving_from)
            WebDriverWait(driver, 5).until(
                lambda d: len(shadow_root.find_elements(
                    By.CSS_SELECTOR, ".otrl-jp__station-autosuggest__item"
                )) > 0
            )
            shadow_root.find_element(
                By.CSS_SELECTOR, ".otrl-jp__station-autosuggest__item"
            ).click()
            t.sleep(3)

            going = shadow_root.find_element(By.NAME, "stationTo")
            going.send_keys(self.going_to)
            WebDriverWait(driver, 5).until(
                lambda d: len(shadow_root.find_elements(
                    By.CSS_SELECTOR, ".otrl-jp__station-autosuggest__item"
                )) > 0
            )
            shadow_root.find_element(
                By.CSS_SELECTOR, ".otrl-jp__station-autosuggest__item"
            ).click()
            t.sleep(3)

            # --- click search ---
            shadow_root.find_element(
                By.CLASS_NAME, "otrl-jp__tickets__submit"
            ).click()
            t.sleep(5)

            # --- scrape results ---
            self.progress.emit("Results page loaded – beginning scrape …")
            wait = WebDriverWait(driver, 5)
            seen_departures: set = set()
            records: list[dict] = []

            # Determine the current displayed date
            try:
                date_text = driver.find_element(
                    By.CSS_SELECTOR, ".service-carousel-header-v2__date"
                ).text.strip()
                current_date = datetime.datetime.strptime(date_text, "%a %d %b %Y").date()
            except Exception as e:
                self.progress.emit(f"  Could not parse date header: {e}")
                current_date = datetime.date.today()

            current_date_tracker = current_date

            # ---- helper ------------------------------------------------
            def parse_current_page():
                nonlocal current_date_tracker
                train_items = driver.find_elements(By.CSS_SELECTOR, ".service-box-v2__item")
                fare_tiles = driver.find_elements(By.CSS_SELECTOR, ".fare-list-v2__tile .price")

                for i, item in enumerate(train_items):
                    try:
                        sr_text = item.find_element(By.CSS_SELECTOR, ".sr-only").text

                        dep_match = re.search(r'^(\d{2}:\d{2})', sr_text)
                        arr_match = re.search(r'arriving at .+? at (\d{2}:\d{2})', sr_text)
                        dur_match = re.search(r'takes (.+?),', sr_text)
                        chg_match = re.search(r'has (\d+) change', sr_text)

                        departure = dep_match.group(1) if dep_match else None
                        if not departure:
                            continue

                        arrival = arr_match.group(1) if arr_match else None
                        dep_time = datetime.datetime.strptime(departure, "%H:%M").time()
                        current_date = current_date_tracker

                        if records:
                            last_dep_dt = records[-1]["departure_dt"]
                            candidate_dt = datetime.datetime.combine(current_date, dep_time)
                            if (last_dep_dt - candidate_dt).total_seconds() > 6 * 3600:
                                current_date = current_date + datetime.timedelta(days=1)
                                current_date_tracker = current_date
                                self.progress.emit(f"  Date rolled over to {current_date}")

                        dedup_key = (current_date, departure)
                        if dedup_key in seen_departures:
                            continue

                        duration = dur_match.group(1).strip() if dur_match else None
                        changes = int(chg_match.group(1)) if chg_match else 0
                        price = None
                        if i < len(fare_tiles):
                            price_text = fare_tiles[i].text.strip()
                            price_match = re.search(r'[\d.]+', price_text)
                            price = float(price_match.group()) if price_match else None

                        departure_dt = datetime.datetime.combine(current_date, dep_time)
                        arrival_dt = None
                        if arrival:
                            arr_time = datetime.datetime.strptime(arrival, "%H:%M").time()
                            arrival_dt = datetime.datetime.combine(current_date, arr_time)
                            if arrival_dt < departure_dt:
                                arrival_dt += datetime.timedelta(days=1)

                        records.append({
                            "departure_dt": departure_dt,
                            "arrival_dt": arrival_dt,
                            "departure": departure,
                            "arrival": arrival,
                            "duration": duration,
                            "changes": changes,
                            "price_gbp": price,
                        })
                        seen_departures.add(dedup_key)
                    except Exception as e:
                        self.progress.emit(f"  Error parsing train {i}: {e}")

            # ---- initial parse ----------------------------------------
            parse_current_page()
            self.progress.emit(f"After initial load: {len(records)} trains")

            # ---- paginate with "Later" button -------------------------
            click_num = 0
            while not self._stop_flag:
                # Check end-date condition
                if current_date_tracker > self.end_date:
                    self.progress.emit(
                        f"Reached end date ({self.end_date}) — stopping."
                    )
                    break

                try:
                    t.sleep(random.uniform(0.3, 1.4))
                    last_departure_before = driver.find_elements(
                        By.CSS_SELECTOR,
                        ".service-box-v2__item .departure-time time",
                    )[-1].get_attribute("datetime")

                    later_btn = wait.until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, "a.service-pager.pull-right")
                        )
                    )
                    driver.execute_script("arguments[0].click();", later_btn)
                    WebDriverWait(driver, 10).until(
                        lambda d: d.find_elements(
                            By.CSS_SELECTOR,
                            ".service-box-v2__item .departure-time time",
                        )[-1].get_attribute("datetime") != last_departure_before
                    )
                except TimeoutException:
                    self.progress.emit("No more 'Later' results — stopping.")
                    break

                parse_current_page()
                click_num += 1
                self.progress.emit(
                    f"After click {click_num}: {len(records)} trains total"
                )

            if self._stop_flag:
                self.progress.emit("Scraping cancelled by user.")

            # ---- build dataframe --------------------------------------
            df = pd.DataFrame(records)
            self.progress.emit(f"Scraping complete — {len(df)} trains collected.")
            self.results.emit(df)

        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

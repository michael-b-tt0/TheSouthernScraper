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
from selenium.webdriver.support.ui import Select, WebDriverWait
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

    def __init__(
        self,
        leaving_from: str,
        going_to: str,
        start_date: datetime.date,
        start_time: str,
        end_date: datetime.date,
        parent=None,
    ):
        super().__init__(parent)
        self.leaving_from = leaving_from
        self.going_to = going_to
        self.start_date = start_date
        self.start_time = start_time
        self.end_date = end_date
        self._stop_flag = False

    # ------------------------------------------------------------------
    def request_stop(self):
        """Call from the main thread to ask the scraper to stop early."""
        self._stop_flag = True

    def _select_outbound_date_time(self, driver, shadow_root):
        """Set the outbound date/time in the Southern date picker."""
        wait = WebDriverWait(driver, 10)

        date_input = shadow_root.find_element(By.CSS_SELECTOR, ".otrl-jp__date-input")
        date_input.click()
        t.sleep(1)

        target_month_year = self.start_date.strftime("%B %Y")
        for _ in range(12):
            current = shadow_root.find_element(
                By.CSS_SELECTOR, ".DayPicker-Caption div"
            ).text.strip()
            if current == target_month_year:
                break
            shadow_root.find_element(
                By.CSS_SELECTOR,
                ".otrl-ui__date-picker__month-selector__button--next",
            ).click()
            t.sleep(0.5)
        else:
            raise ValueError(f"Could not navigate to {target_month_year}")

        days = shadow_root.find_elements(
            By.CSS_SELECTOR,
            ".DayPicker-Day:not(.DayPicker-Day--disabled):not(.DayPicker-Day--outside)",
        )
        target_day = str(self.start_date.day)
        for day in days:
            if day.text.strip() == target_day:
                day.click()
                t.sleep(0.5)
                break
        else:
            raise ValueError(f"Could not select day {target_day} in {target_month_year}")

        time_dropdown = shadow_root.find_element(
            By.CSS_SELECTOR,
            "select.otrl-jp__time-input--hour-minute",
        )
        hour, minute = map(int, self.start_time.split(":"))
        if minute not in (0, 15, 30, 45):
            raise ValueError(
                f"Minute must be 00, 15, 30 or 45. Got: {minute:02d}"
            )
        time_index = (hour * 4) + (minute // 15) + 1
        Select(time_dropdown).select_by_index(time_index)
        t.sleep(0.3)

        shadow_root.find_element(
            By.CSS_SELECTOR, ".otrl-jp__date-popup__button"
        ).click()
        wait.until(
            lambda d: shadow_root.find_element(By.CSS_SELECTOR, ".otrl-jp__date-input")
        )
        self.progress.emit(
            f"Selected outbound date/time: {self.start_date:%Y-%m-%d} {self.start_time}"
        )

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

            # --- select outbound date/time ---
            self.progress.emit(
                f"Setting outbound date/time to {self.start_date:%Y-%m-%d} {self.start_time} …"
            )
            self._select_outbound_date_time(driver, shadow_root)

            # --- click search ---
            shadow_root.find_element(
                By.CLASS_NAME, "otrl-jp__tickets__submit"
            ).click()
            t.sleep(5)

            # --- scrape results ---
            self.progress.emit("Results page loaded – beginning scrape …")
            
            # Switch to list view
            self.progress.emit("Switching to list view …")
            listview = driver.find_element(By.CSS_SELECTOR, 'a[aria-label="List view"]')
            listview.click()
            t.sleep(3)

            wait = WebDriverWait(driver, 5)
            seen_departures: set = set()
            records: list[dict] = []

            # Determine the current displayed date
            try:
                date_text = driver.find_element(
                    By.CSS_SELECTOR, ".service-list__heading2"
                ).text.strip()
                current_date = datetime.datetime.strptime(date_text, "%a %d %b %Y").date()
            except Exception as e:
                self.progress.emit(f"  Could not parse date header: {e}")
                current_date = datetime.date.today()

            current_date_tracker = current_date

            # ---- helper ------------------------------------------------
            def parse_current_page():
                nonlocal current_date_tracker
                cards = driver.find_elements(By.CSS_SELECTOR, ".service-list__card.service-fare")

                for i, card in enumerate(cards):
                    try:
                        # Journey info from sr-text h4
                        sr_text = card.find_element(By.CSS_SELECTOR, "h4.sr-text").text

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
                        try:
                            # Target the sr-text span inside the button for a clean price string
                            price_element = card.find_element(By.CSS_SELECTOR, ".btn-continue .sr-text")
                            price_text = price_element.text  
                            # Clean the string (remove £) and convert to float
                            price = float(price_text.replace('£', '').strip())
                        except Exception:
                            pass

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

            def log_later_timeout(stage: str):
                """Emit lightweight diagnostics when pagination stops responding."""
                try:
                    pager_buttons = driver.find_elements(
                        By.CSS_SELECTOR, "a.service-pager[aria-label='Show later trains']"
                    )
                    button_count = len(pager_buttons)
                    button_labels = [btn.text.strip() or "<no text>" for btn in pager_buttons[:2]]
                except Exception as e:
                    button_count = -1
                    button_labels = [f"<error reading buttons: {e}>"]

                try:
                    visible_times = driver.find_elements(
                        By.CSS_SELECTOR, ".service-list-v2__services li .service-summary__station time"
                    )
                    last_visible_time = (
                        visible_times[-1].get_attribute("datetime") if visible_times else None
                    )
                except Exception as e:
                    last_visible_time = f"<error reading times: {e}>"

                self.progress.emit(
                    f"Click {click_num + 1}: timeout while waiting for {stage}."
                )
                self.progress.emit(
                    f"  Diagnostics: url={driver.current_url} | later_buttons={button_count} | "
                    f"last_time={last_visible_time} | labels={button_labels}"
                )

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
                    t.sleep(random.uniform(1.2, 2.2))
                    time_elements = driver.find_elements(
                        By.CSS_SELECTOR, ".service-list-v2__services li .service-summary__station time"
                    )
                    if not time_elements:
                        self.progress.emit(f"Click {click_num + 1}: no time elements found, stopping.")
                        break
                    last_departure_before = time_elements[-1].get_attribute("datetime")

                    try:
                        later_btn = wait.until(
                            EC.element_to_be_clickable(
                                (By.CSS_SELECTOR, "a.service-pager[aria-label='Show later trains']")
                            )
                        )
                    except TimeoutException:
                        log_later_timeout("the 'Later' button to become clickable")
                        break

                    driver.execute_script("arguments[0].click();", later_btn)
                    
                    def page_has_updated(d):
                        try:
                            elements = d.find_elements(
                                By.CSS_SELECTOR, ".service-list-v2__services li .service-summary__station time"
                            )
                            return (
                                len(elements) > 0
                                and elements[-1].get_attribute("datetime") != last_departure_before
                            )
                        except Exception:
                            return False

                    try:
                        WebDriverWait(driver, 10).until(page_has_updated)
                    except TimeoutException:
                        log_later_timeout("the results list to change after clicking 'Later'")
                        break

                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, ".service-list-v2__services li .btn-continue")
                            )
                        )
                    except TimeoutException:
                        log_later_timeout("refreshed fare buttons to appear")
                        break

                except Exception as e:
                    self.progress.emit(f"Click {click_num + 1}: pagination failed: {e}")
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

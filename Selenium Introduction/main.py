import csv
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class WebDriverContext:
    def __enter__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.driver.quit()
        except:
            pass

BASE_DIR = r"C:/Users/mariia_onyshchenko/dqe_automation/dqe-automation/Selenium Introduction"
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "screenshots")
CSV_DIR = os.path.join(BASE_DIR, "csv")
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

def extract_table(driver, csv_dir):
    try:
        wait = WebDriverWait(driver, 10)

        table = None
        try:
            #by id
            table = driver.find_element(By.ID, "table")
        except:
            try:
                #by css
                table = driver.find_element(By.CSS_SELECTOR, "table")
            except:
                try:
                    #by xpath
                    table = driver.find_element(By.XPATH, "//table")
                except:
                    table = None

        rows = []
        if table:
            for tr in table.find_elements(By.TAG_NAME, "tr"):
                cells = tr.find_elements(By.TAG_NAME, "th") or tr.find_elements(By.TAG_NAME, "td")
                row = [cell.text.strip() for cell in cells]
                if row:
                    rows.append(row)
        else:
            #fallback to plotly extraction
            plotly_data = driver.execute_script("""
                let gd = document.querySelector('.plotly-graph-div');
                if (!gd || !gd.data) return null;
                for (let i = 0; i < gd.data.length; i++) {
                    if (gd.data[i].type === "table") return gd.data[i];
                }
                return null;
            """)
            if not plotly_data:
                print("table not found")
                return
            header = plotly_data["header"]["values"]
            cells = plotly_data["cells"]["values"]
            rows = [header] + list(zip(*cells))

        csv_path = os.path.join(csv_dir, "table.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        print("table extracted: ", csv_path)
    except Exception as e:
        print("table extraction failed: ", e)

def extract_donut_chart(driver, screenshots_dir, csv_dir):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    try:
        wait = WebDriverWait(driver, 10)
        chart_container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.plot-container"))
        )

        def scroll_chart(el):
            driver.execute_script("""
                arguments[0].scrollIntoView({block: 'center', inline: 'center'});
            """, el)
            time.sleep(0.5) 

        def extract_slices():
            slices = driver.find_elements(By.CSS_SELECTOR, "g.slice")
            data = []
            total = 0
            for sl in slices:
                try:
                    text_el = sl.find_element(By.CSS_SELECTOR, "g.slicetext text")
                    tspan_elements = text_el.find_elements(By.TAG_NAME, "tspan")
                    if len(tspan_elements) >= 2:
                        label = tspan_elements[0].text.strip()
                        value = int(tspan_elements[1].text.strip())
                        data.append({"label": label, "value": value})
                        total += value
                except:
                    continue
            for d in data:
                d["percentage"] = round(d["value"] / total * 100, 1) if total > 0 else 0
            return data

        def save_csv(data, counter):
            csv_path = os.path.join(csv_dir, f"doughnut{counter}.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Label", "Value", "Percentage"])
                for d in data:
                    writer.writerow([d["label"], d["value"], f"{d['percentage']}%"])
            print("doughnut chart data saved")

        screenshot_counter = 0
        scroll_chart(chart_container)
        chart_container.screenshot(os.path.join(screenshots_dir, f"screenshot{screenshot_counter}.png"))
        save_csv(extract_slices(), screenshot_counter)
        print("initial chart screenshot taken")

        legends = driver.find_elements(By.CSS_SELECTOR, "g.legend g.traces")
        if not legends:
            print("no legend filters found")
            return

        print(f"found {len(legends)} legend filters")
        screenshot_counter += 1

        for idx, legend in enumerate(legends):
            try:
                scroll_chart(legend)
                if legend.is_displayed() and legend.is_enabled():
                    legend.click()
                    time.sleep(0.5) 
                    scroll_chart(chart_container)
                    chart_container.screenshot(os.path.join(screenshots_dir, f"screenshot{screenshot_counter}.png"))
                    save_csv(extract_slices(), screenshot_counter)

                    screenshot_counter += 1
            except Exception as e:
                print(f"legend {idx} click failed: {e}")
                continue

        print("doughnut chart extraction completed")

    except Exception as e:
        print(f"doughnut chart extraction failed: {e}")

if __name__ == "__main__":
    local_html = os.path.join(BASE_DIR, "report.html")
    html_path = f"file:///{local_html.replace(os.sep, '/')}"

    with WebDriverContext() as driver:
        driver.get(html_path)
        time.sleep(1)

        extract_table(driver, CSV_DIR)
        extract_donut_chart(driver, SCREENSHOTS_DIR, CSV_DIR)

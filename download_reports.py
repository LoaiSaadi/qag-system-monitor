import time
from pathlib import Path
import os

from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver


# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

EMAIL = os.getenv("PBI_EMAIL")
PASSWORD = os.getenv("PBI_PASSWORD")
URL = os.getenv("PBI_URL")

REPORT1_URL = os.getenv("PBI_REPORT1_URL")
REPORT2_URL = os.getenv("PBI_REPORT2_URL")


# ---------------------------
# Passkey / WebAuthn blocker
# ---------------------------
def disable_passkey_enrollment(driver: webdriver.Chrome) -> None:
    """
    Blocks passkey enrollment (navigator.credentials.create), which triggers
    the Windows Security 'Choose where to save this passkey' popup.
    """
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": """
            (() => {
              try {
                if (navigator.credentials && navigator.credentials.create) {
                  navigator.credentials.create = async () => {
                    throw new Error("Passkey enrollment disabled by automation");
                  };
                }
              } catch (e) {}
            })();
        """},
    )


# ---------------------------
# Driver setup (Chrome)
# ---------------------------
def create_chrome_driver(download_dir: str, headless: bool = False) -> webdriver.Chrome:
    download_dir = str(Path(download_dir).resolve())
    Path(download_dir).mkdir(parents=True, exist_ok=True)

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,1000")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    # Reduce WebAuthn conditional UI prompts
    options.add_argument("--disable-features=WebAuthenticationConditionalUI")

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        # helps when multiple downloads happen in one session
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    # IMPORTANT: inject BEFORE first navigation
    disable_passkey_enrollment(driver)

    return driver


# ---------------------------
# Download wait helper
# ---------------------------
def wait_for_new_download(download_dir: str, timeout_sec: int = 120) -> Path:
    """
    Wait until a new file appears in download_dir and is fully downloaded (no .crdownload).
    Returns the downloaded file path.
    """
    download_path = Path(download_dir)
    before = {p.name for p in download_path.glob("*")}

    start = time.time()
    while time.time() - start < timeout_sec:
        if list(download_path.glob("*.crdownload")):
            time.sleep(0.5)
            continue

        after_files = [p for p in download_path.glob("*") if p.is_file()]
        new_files = [p for p in after_files if p.name not in before]

        if new_files:
            new_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return new_files[0]

        time.sleep(0.5)

    raise TimeoutError(f"No completed download detected within {timeout_sec}s in {download_dir}")


def _wait_for_report_canvas(wait: WebDriverWait):
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//*[contains(@class,'report') or contains(@class,'canvas') or contains(@aria-label,'Report')]")
        )
    )


def export_visual_to_csv(
    driver: webdriver.Chrome,
    report_url: str,
    download_dir: str,
    target_name: str,
    timeout_sec: int = 60,
) -> Path:
    print(f"Exporting visual to CSV from: {report_url}")
    wait = WebDriverWait(driver, timeout_sec)
    driver.get(report_url)

    _wait_for_report_canvas(wait)
    print("Report canvas loaded.")

    actions = ActionChains(driver)

    try:
        visual_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "visual-container")))
        actions.move_to_element_with_offset(visual_box, 10, 10).perform()
        time.sleep(1)
        actions.move_by_offset(50, 0).perform()

        more_options_btn = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-testid='visual-more-options-btn']"))
        )
        driver.execute_script("arguments[0].click();", more_options_btn)
        time.sleep(1)

        export_item = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-testid='pbimenu-item.Export data']"))
        )
        driver.execute_script("arguments[0].click();", export_item)

        # Summarized data
        try:
            summarized_input = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[aria-label='Summarized data']"))
            )
            driver.execute_script("arguments[0].click();", summarized_input)
        except:
            summarized_span = wait.until(EC.presence_of_element_located((By.XPATH, "//span[text()='Summarized data']")))
            driver.execute_script("arguments[0].click();", summarized_span)

        time.sleep(0.5)

        # File format dropdown
        file_format_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-testid='pbi-dropdown']")))
        driver.execute_script("arguments[0].click();", file_format_btn)
        time.sleep(0.5)

        # Select .csv
        try:
            csv_option = wait.until(EC.presence_of_element_located((By.XPATH, "//pbi-dropdown-item[contains(., '.csv')]")))
            driver.execute_script("arguments[0].scrollIntoView(true);", csv_option)
            time.sleep(0.3)
            internal_div = csv_option.find_element(By.CSS_SELECTOR, "div[role='option']")
            driver.execute_script("arguments[0].click();", internal_div)
        except:
            # fallback xpath (brittle but kept)
            fallback_option = driver.find_element(By.XPATH, "/html/body/div[2]/div[6]/div/pbi-dropdown-overlay/div/div/pbi-dropdown-item[2]")
            driver.execute_script("arguments[0].click();", fallback_option)

        time.sleep(0.8)

        # Final Export
        export_final = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[2]/div[4]/div/mat-dialog-container/div/div/export-data-dialog/mat-dialog-actions/button[1]")
            )
        )
        driver.execute_script("arguments[0].click();", export_final)
        print("Export initiated successfully!")

    except Exception as e:
        raise RuntimeError(f"Export flow failed: {e}")

    downloaded_file = wait_for_new_download(download_dir, timeout_sec=180)
    print(f"Downloaded: {downloaded_file}")

    new_path = Path(download_dir) / f"{target_name}.csv"
    if new_path.exists():
        new_path.unlink()
    downloaded_file.rename(new_path)

    print(f"Renamed to: {new_path.name}")
    return new_path


def download_report1(driver: webdriver.Chrome, report1_url: str, download_dir: str) -> Path:
    print("Downloading report 1...")
    return export_visual_to_csv(driver, report1_url, download_dir, target_name="Health Algo Daily Statistics")


def download_report2(driver: webdriver.Chrome, report2_url: str, download_dir: str) -> Path:
    print("Downloading report 2...")
    return export_visual_to_csv(driver, report2_url, download_dir, target_name="Health Algo Commands Daily Statistics")


def login_to_pbi(driver, wait, email, password):
    print("Logging in...")

    email_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='email']")))
    email_input.clear()
    email_input.send_keys(email)

    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='submitBtn']"))).click()

    # This is where the passkey popup used to appear.
    # With the WebAuthn blocker, it should not trigger now.

    use_password_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//*[contains(text(), 'Use your password')] | //*[@id='signInAnotherWay']")
        )
    )
    use_password_link.click()

    password_field = wait.until(
        EC.visibility_of_element_located((By.XPATH, "//*[@id='passwordEntry'] | //*[@id='i0111']"))
    )
    password_field.send_keys(password)
    password_field.send_keys(Keys.ENTER)

    time.sleep(2)
    try:
        stay_signed_in = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"view\"]/div/div[5]/button[1]")))
        stay_signed_in.click()
        time.sleep(3)
    except:
        pass


if __name__ == "__main__":
    # Basic validation so it fails fast if env is missing
    missing = [k for k, v in {
        "PBI_EMAIL": EMAIL,
        "PBI_PASSWORD": PASSWORD,
        "PBI_URL": URL,
        "PBI_REPORT1_URL": REPORT1_URL,
        "PBI_REPORT2_URL": REPORT2_URL,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing required env vars: {missing}")

    DOWNLOAD_DIR = "./reports"

    driver = create_chrome_driver(DOWNLOAD_DIR, headless=False)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(URL)
        login_to_pbi(driver, wait, EMAIL, PASSWORD)

        r1 = download_report1(driver, REPORT1_URL, DOWNLOAD_DIR)
        r2 = download_report2(driver, REPORT2_URL, DOWNLOAD_DIR)

        print("Done:", r1.name, r2.name)
    finally:
        driver.quit()
import time
from datetime import datetime
from typing import List, Dict, Optional
import dateparser
import re

from pymongo import MongoClient, UpdateOne
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- MongoDB ----------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "foody2-db"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
reviews_col = db["reviews"]

# ---------------- Helper ----------------
def now_date_str():
    return datetime.now().strftime("%Y-%m-%d")

def to_iso(dt_text: Optional[str]) -> Optional[str]:
    if not dt_text:
        return None
    dt = dateparser.parse(dt_text, languages=["vi"], settings={"TIMEZONE": "Asia/Ho_Chi_Minh"})
    return dt.isoformat() if dt else None

def safe_text(el) -> str:
    try:
        return el.text.strip()
    except Exception:
        return ""

def find_or_none(driver, by, value):
    try:
        return driver.find_element(by, value)
    except NoSuchElementException:
        return None

def find_all(driver, by, value) -> List:
    try:
        return driver.find_elements(by, value)
    except NoSuchElementException:
        return []

# ---------------- Selenium Setup ----------------
def setup_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=vi-VN")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
    driver.set_page_load_timeout(60)
    return driver

# ---------------- Login Foody ----------------
def foody_login(driver, email, password):
    login_url = "https://www.foody.vn/account/login"
    driver.get(login_url)
    time.sleep(3)
    try:
        driver.find_element(By.ID, "email").send_keys(email)
        driver.find_element(By.ID, "password").send_keys(password)
        driver.find_element(By.XPATH, '//button[contains(text(),"Đăng nhập")]').click()
        time.sleep(5)
        print("Đăng nhập thành công!")
    except Exception as e:
        print("Lỗi khi login:", e)

# ---------------- Extract Quận ----------------
def extract_district_from_address(address: str) -> Optional[str]:
    if not address:
        return None
    match = re.search(r'(Quận\s*[0-9]+|Quận\s*(Bình\s*Thạnh|Tân\s*Bình|Phú\s*Nhuận|Gò\s*Vấp|Bình\s*Tân|Tân\s*Phú)|TP\.\s*Thủ\s*Đức|Huyện\s*[\w\s]+)', address, re.IGNORECASE)
    if match:
        district = match.group(0).strip()
        district = district.replace("q.", "Quận").replace("Q.", "Quận").title()
        if "tp." in district.lower():
            district = "TP. Thủ Đức"
        return district
    return None

# ---------------- Cào danh sách quán ----------------
BASE_URL = "https://www.foody.vn/ho-chi-minh/quan-an"

def list_restaurants_general(driver, max_pages: int = 100) -> List[Dict]:
    restaurants = []
    for attempt in range(3):
        try:
            driver.get(BASE_URL)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            break
        except TimeoutException:
            print(f"Timeout khi load {BASE_URL}, thử lại {attempt+1}/3")
            time.sleep(5)
    else:
        print("Không thể load trang danh sách quán.")
        return restaurants

    for page in range(1, max_pages + 1):
        print(f"Trang {page}...")
        cards = find_all(driver, By.CSS_SELECTOR, "a[href*='/ho-chi-minh/'][title]:not([href*='/thanh-vien/']):not([href*='/tag/'])")
        print(f"  → {len(cards)} quán trên trang")
        for card in cards:
            try:
                url = card.get_attribute("href")
                name = card.get_attribute("title") or safe_text(card.find_element(By.CSS_SELECTOR, "h2, .name-res"))
                address_el = find_or_none(card, By.CSS_SELECTOR, ".address, .location, span[itemprop='streetAddress']")
                address = safe_text(address_el) if address_el else ""
                district = extract_district_from_address(address)
                restaurants.append({
                    "name": name.strip() if name else url.split("/")[-1].replace("-", " ").title(),
                    "url": url,
                    "district": district or "Không xác định",
                    "address": address
                })
            except Exception:
                continue

        next_btn = find_or_none(driver, By.CSS_SELECTOR, "a.nextpage, a[rel='next'], .pager a:last-child")
        if next_btn and next_btn.is_enabled():
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(4)
        else:
            break

    # Dedupe
    seen = set()
    uniq = []
    for r in restaurants:
        if r["url"] not in seen:
            seen.add(r["url"])
            uniq.append(r)
    print(f"Tổng quán duy nhất: {len(uniq)}")
    return uniq

# ---------------- Cào review ----------------
def open_review_tab_if_exists(driver):
    candidates = find_all(driver, By.CSS_SELECTOR, "a.tab-link, a[href*='#review'], a[data-target*='review']")
    for a in candidates:
        if "đánh giá" in safe_text(a).lower() or "review" in safe_text(a).lower():
            try:
                driver.execute_script("arguments[0].click();", a)
                time.sleep(2)
                return True
            except Exception:
                continue
    return False

def parse_review_item(driver, item, restaurant) -> Dict:
    commenter_name = safe_text(find_or_none(item, By.CSS_SELECTOR, "a.username, a[href*='/thanh-vien/']"))
    comment_text = safe_text(find_or_none(item, By.CSS_SELECTOR, ".review-content, .rd-des, .text"))
    vc = find_or_none(item, By.CSS_SELECTOR, ".useful-count, .like-count")
    vote_count = int(vc.text.strip().replace(",", "")) if vc and vc.text.strip().isdigit() else 0
    rating_el = find_or_none(item, By.CSS_SELECTOR, ".review-points, .point")
    rating = float(rating_el.text.strip().replace(",", ".")) if rating_el else None
    time_el = find_or_none(item, By.CSS_SELECTOR, ".review-date, .date")
    comment_time = to_iso(safe_text(time_el)) if time_el else None
    imgs = [img.get_attribute("src") for img in find_all(item, By.CSS_SELECTOR, ".review-photos img, img[data-original]") if img.get_attribute("src")]
    commenter_id = None
    return {
        "restaurant_name": restaurant.get("name"),
        "restaurant_url": restaurant.get("url"),
        "district": restaurant.get("district"),
        "address": restaurant.get("address"),
        "commenter_name": commenter_name,
        "comment_text": comment_text,
        "comment_time": comment_time,
        "vote_count": vote_count,
        "rating": rating,
        "comment_images": imgs,
        "crawl_date": now_date_str()
    }

def extract_reviews_from_page(driver, restaurant) -> List[Dict]:
    items = find_all(driver, By.CSS_SELECTOR, ".review-item, .microsite-review-item, li.review")
    return [parse_review_item(driver, it, restaurant) for it in items if parse_review_item(driver, it, restaurant)["comment_text"]]

def crawl_reviews_for_restaurant(driver, restaurant, max_pages: int = 50) -> List[Dict]:
    driver.get(restaurant["url"])
    time.sleep(5)
    open_review_tab_if_exists(driver)
    all_reviews = []
    for _ in range(max_pages):
        page_reviews = extract_reviews_from_page(driver, restaurant)
        all_reviews.extend(page_reviews)
        next_btn = find_or_none(driver, By.CSS_SELECTOR, "a.nextpage, a[title='Trang sau']")
        if next_btn and next_btn.is_enabled():
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(3)
        else:
            break
    all_reviews.sort(key=lambda x: x["comment_time"] or "", reverse=True)
    return all_reviews

def upsert_reviews(reviews: List[Dict]):
    if not reviews:
        return
    ops = []
    for rv in reviews:
        key = {
            "restaurant_url": rv["restaurant_url"],
            "commenter_name": rv["commenter_name"],
            "comment_text": rv["comment_text"],
            "comment_time": rv["comment_time"],
        }
        ops.append(UpdateOne(key, {"$set": rv}, upsert=True))
    if ops:
        reviews_col.bulk_write(ops, ordered=False)

# ---------------- Orchestrator ----------------
def crawl_general_and_auto_district(email, password,
                                   restaurants_limit: int = 500,
                                   review_pages_per_restaurant: int = 30,
                                   max_list_pages: int = 80,
                                   headless: bool = False):
    driver = setup_driver(headless=headless)
    total_reviews = 0
    try:
        foody_login(driver, email, password)
        restaurants = list_restaurants_general(driver, max_pages=max_list_pages)
        from collections import Counter
        districts = [r["district"] for r in restaurants if r["district"]]
        print("Phân bố quận:", Counter(districts).most_common(10))

        for idx, r in enumerate(restaurants[:restaurants_limit]):
            print(f"[{idx+1}/{min(len(restaurants), restaurants_limit)}] {r['name']} - {r['district']}")
            reviews = crawl_reviews_for_restaurant(driver, r, max_pages=review_pages_per_restaurant)
            upsert_reviews(reviews)
            total_reviews += len(reviews)
            print(f" → {len(reviews)} reviews (tổng: {total_reviews})")
            time.sleep(2)

        print(f"Hoàn thành! Tổng {len(restaurants)} quán, {total_reviews} reviews")
    finally:
        driver.quit()

# ---------------- MAIN ----------------
if __name__ == "__main__":
    EMAIL = "nguyenthisuonganh11a4tq04@gmail.com"
    PASSWORD = "02052707Anh@"
    crawl_general_and_auto_district(
        EMAIL,
        PASSWORD,
        restaurants_limit=300,
        review_pages_per_restaurant=40,
        max_list_pages=100,
        headless=False
    )

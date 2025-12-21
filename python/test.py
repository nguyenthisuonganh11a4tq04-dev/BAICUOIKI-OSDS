import time
from datetime import datetime
from typing import List, Dict, Optional
import dateparser

from pymongo import MongoClient, UpdateOne
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service

# ----------------------------- Cấu hình DB -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "foody1-db"
CITY_BASE_URL = "https://www.foody.vn/ho-chi-minh"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
reviews_col = db["reviews"]
restaurants_col = db["restaurants"]
foods_col = db["foods"]  # Bảng món ăn riêng biệt

# ----------------------------- Tiện ích -----------------------------
def now_date_str():
    return datetime.now().strftime("%Y-%m-%d")

def to_iso(dt_text: Optional[str]) -> Optional[str]:
    if not dt_text:
        return None
    dt = dateparser.parse(dt_text, languages=["vi"], settings={"TIMEZONE": "Asia/Ho_Chi_Minh"})
    return dt.isoformat() if dt else None

def setup_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=vi-VN")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver

def safe_text(el) -> str:
    try: return el.text.strip()
    except: return ""

def find_or_none(driver, by, value):
    try: return driver.find_element(by, value)
    except NoSuchElementException: return None

# ----------------------------- Logic Cào Dữ Liệu -----------------------------

def crawl_restaurant_and_foods(driver, url: str):
    """Cào chi tiết quán và tách danh sách món ăn ra bảng riêng."""
    driver.get(url)
    time.sleep(3)
    
    name = safe_text(find_or_none(driver, By.CSS_SELECTOR, "h1.main-info-title, .name-res"))
    address = safe_text(find_or_none(driver, By.CSS_SELECTOR, ".res-common-add, .address-res"))
    
    # Bộ lọc rác để tránh cào nhầm menu hệ thống/footer
    trash_keywords = [
        "Giới thiệu", "Trung tâm", "Quy chế", "Điều khoản", 
        "Liên hệ", "Góp ý", "Trợ giúp", "Chính sách", 
        "Tuyển dụng", "Ứng dụng", "ShopeePay", "Phí dịch vụ"
    ]
    
    res_doc = {
        "url": url,
        "name": name,
        "address": address,
        "last_updated": now_date_str()
    }

    food_items = []
    dish_els = driver.find_elements(By.CSS_SELECTOR, 
        ".menu-item-name, .txt-menu-item, .item-restaurant-name, .name-food, .dish-name"
    )
    
    for d in dish_els:
        food_name = safe_text(d)
        if food_name and 2 < len(food_name) < 100:
            if not any(k.lower() in food_name.lower() for k in trash_keywords):
                food_items.append({
                    "restaurant_url": url,
                    "food_name": food_name,
                    "crawl_date": now_date_str()
                })

    return res_doc, food_items

def get_latest_review_time(url: str) -> Optional[str]:
    latest = reviews_col.find_one({"restaurant_url": url}, sort=[("comment_time", -1)])
    return latest["comment_time"] if latest else None

def crawl_reviews_incremental(driver, res_url, max_pages=5):
    latest_db_time = get_latest_review_time(res_url)
    new_reviews = []
    
    for tab_name in ["Đánh giá", "Bình luận", "Reviews"]:
        tab_btn = find_or_none(driver, By.PARTIAL_LINK_TEXT, tab_name)
        if tab_btn:
            driver.execute_script("arguments[0].click();", tab_btn)
            time.sleep(1.5)
            break

    for p in range(max_pages):
        items = driver.find_elements(By.CSS_SELECTOR, ".review-item, .comment-item, .rd-item")
        if not items: break
        
        reached_old_data = False
        for it in items:
            try:
                content = safe_text(find_or_none(it, By.CSS_SELECTOR, ".review-text, .rd-des"))
                time_str = safe_text(find_or_none(it, By.CSS_SELECTOR, ".time, .date"))
                iso_time = to_iso(time_str)
                
                if latest_db_time and iso_time and iso_time <= latest_db_time:
                    reached_old_data = True
                    break
                
                if content:
                    new_reviews.append({
                        "restaurant_url": res_url,
                        "commenter_name": safe_text(find_or_none(it, By.CSS_SELECTOR, ".user-name, .username")),
                        "comment_text": content,
                        "comment_time": iso_time,
                        "crawl_date": now_date_str()
                    })
            except: continue
                
        if reached_old_data: break
            
        next_btn = find_or_none(driver, By.CSS_SELECTOR, "a.next")
        if next_btn:
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(1.5)
        else: break
            
    return new_reviews

# ----------------------------- Lưu Trữ -----------------------------

def save_to_mongodb(res_doc, food_items, reviews):
    # 1. Lưu quán ăn
    restaurants_col.update_one({"url": res_doc["url"]}, {"$set": res_doc}, upsert=True)
    
    # 2. Lưu món ăn vào bảng foods (tách biệt)
    if food_items:
        food_ops = [
            UpdateOne(
                {"restaurant_url": f["restaurant_url"], "food_name": f["food_name"]}, 
                {"$set": f}, 
                upsert=True
            ) for f in food_items
        ]
        foods_col.bulk_write(food_ops, ordered=False)

    # 3. Lưu reviews
    if reviews:
        rev_ops = [
            UpdateOne(
                {"restaurant_url": r["restaurant_url"], "comment_text": r["comment_text"], "comment_time": r["comment_time"]}, 
                {"$set": r}, 
                upsert=True
            ) for r in reviews
        ]
        reviews_col.bulk_write(rev_ops, ordered=False)

# ----------------------------- Chạy chính -----------------------------

def run_crawler():
    driver = setup_driver(headless=True)
    try:
        driver.get(CITY_BASE_URL)
        time.sleep(3)
        
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/ho-chi-minh/']")
        urls = list(set([l.get_attribute("href") for l in links if "/ho-chi-minh/" in l.get_attribute("href")]))[:30]

        for url in urls:
            print(f"Đang xử lý: {url}")
            try:
                res_doc, food_items = crawl_restaurant_and_foods(driver, url)
                new_reviews = crawl_reviews_incremental(driver, url)
                
                save_to_mongodb(res_doc, food_items, new_reviews)
                print(f"   + Đã lưu: {len(food_items)} món ăn và {len(new_reviews)} review mới.")
            except Exception as e:
                print(f"   ! Lỗi tại {url}: {e}")
                continue
    finally:
        driver.quit()

if __name__ == "__main__":
    run_crawler()
import time
import json
from datetime import datetime
from typing import List, Dict, Optional
import dateparser

from pymongo import MongoClient, UpdateOne
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "foody-db"
CITY_BASE_URL = "https://www.foody.vn/ho-chi-minh"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
reviews_col = db["reviews"]
users_col = db["users"]

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
        # Headless mode mới trong Chrome
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=vi-VN")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
    )

    # Sử dụng Service thay vì truyền trực tiếp đường dẫn
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver



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

# ----------------------------- Nguồn khởi đầu -----------------------------

def list_restaurants_hcm(driver, max_pages: int = 40) -> List[Dict]:
    restaurants = []
    driver.get(CITY_BASE_URL)
    time.sleep(2)

    # Có thể cần vào mục "Địa điểm" hoặc lọc quán ăn
    # Dưới đây dùng selector tương đối; bạn nên kiểm tra DOM thực tế
    for page_idx in range(max_pages):
        cards = find_all(driver, By.CSS_SELECTOR, ".fdc-item, .fdc-card, .content-item a[href*='/ho-chi-minh/']")
        for c in cards:
            try:
                anchor = c if c.get_attribute("href") else c.find_element(By.CSS_SELECTOR, "a")
                url = anchor.get_attribute("href")
                name = safe_text(c)
                if url and "/ho-chi-minh/" in url:
                    restaurants.append({"name": name or url.split("/")[-1], "url": url})
            except Exception:
                continue

        # Phân trang
        next_btn = find_or_none(driver, By.CSS_SELECTOR, ".pagination a.next, a.next")
        if next_btn and next_btn.is_enabled():
            next_btn.click()
            time.sleep(1.5)
        else:
            break

    # dedupe theo URL
    seen = set()
    uniq = []
    for r in restaurants:
        if r["url"] not in seen:
            seen.add(r["url"])
            uniq.append(r)
    return uniq

def list_featured_dishes_hcm(driver, max_pages: int = 3) -> List[Dict]:
    featured = []
    # Trang đề cử món ăn (ví dụ)
    driver.get("https://www.foody.vn/ho-chi-minh/goi-y-mon-ngon")
    time.sleep(2)

    for page_idx in range(max_pages):
        items = find_all(driver, By.CSS_SELECTOR, ".fdc-item, .dish-item, .content-item a")
        for it in items:
            try:
                anchor = it if it.get_attribute("href") else it.find_element(By.CSS_SELECTOR, "a")
                url = anchor.get_attribute("href")
                name = safe_text(it)
                if url and "/ho-chi-minh/" in url:
                    featured.append({"name": name or url.split("/")[-1], "url": url})
            except Exception:
                continue

        next_btn = find_or_none(driver, By.CSS_SELECTOR, ".pagination a.next, a.next")
        if next_btn and next_btn.is_enabled():
            next_btn.click()
            time.sleep(1.5)
        else:
            break

    # dedupe
    seen = set()
    uniq = []
    for d in featured:
        if d["url"] not in seen:
            seen.add(d["url"])
            uniq.append(d)
    return uniq

# ----------------------------- Cào reviews -----------------------------

def open_review_tab_if_exists(driver):
    # thử các tab có chữ "Đánh giá" hoặc "Bình luận"
    candidates = find_all(driver, By.CSS_SELECTOR, "a")
    for a in candidates:
        txt = safe_text(a).lower()
        if "đánh giá" in txt or "bình luận" in txt or "review" in txt:
            try:
                driver.execute_script("arguments[0].click();", a)
                time.sleep(1.2)
                return True
            except Exception:
                continue
    return False

def parse_review_item(driver, item, restaurant) -> Dict:
    # Các selector có thể cần chỉnh theo DOM thực tế
    commenter_name = ""
    comment_text = ""
    vote_count = 0
    rating = None
    comment_time = None
    comment_images = []
    reviewer_profile_url = None
    review_id = None

    # Người cmt và profile
    try:
        user_el = item.find_element(By.CSS_SELECTOR, ".user-name a, .username a, .author a")
        commenter_name = safe_text(user_el)
        reviewer_profile_url = user_el.get_attribute("href")
        # commenter_id từ URL (nếu có)
    except NoSuchElementException:
        commenter_name = safe_text(find_or_none(item, By.CSS_SELECTOR, ".user-name, .username, .author"))

    # Nội dung
    comment_text = safe_text(find_or_none(item, By.CSS_SELECTOR, ".review-text, .rd-des, .content, .desc"))

    # Vote
    vc_el = find_or_none(item, By.CSS_SELECTOR, ".like-count, .vote-count, .count .like")
    if vc_el:
        try:
            vote_count = int(safe_text(vc_el).split()[0])
        except Exception:
            vote_count = 0

    # Rating (data-rating hoặc số sao)
    r_el = find_or_none(item, By.CSS_SELECTOR, ".stars, .rating, .review-rating [data-rating]")
    if r_el:
        try:
            data_rating = r_el.get_attribute("data-rating")
            rating = float(data_rating) if data_rating else None
        except Exception:
            # fallback: đếm sao đã chọn
            stars = find_all(item, By.CSS_SELECTOR, ".stars i.active, .rating i.selected")
            rating = float(len(stars)) if stars else None

    # Thời gian
    t_el = find_or_none(item, By.CSS_SELECTOR, ".time, .date, .created-at")
    if t_el:
        comment_time = to_iso(safe_text(t_el))

    # Ảnh
    for img in find_all(item, By.CSS_SELECTOR, "img"):
        src = img.get_attribute("src")
        if src and "image.foody.vn" in src:
            comment_images.append(src)

    # review_id nếu có data-id
    try:
        review_id = item.get_attribute("data-id") or None
    except Exception:
        pass

    doc = {
        "restaurant_id": restaurant.get("id"),
        "restaurant_name": restaurant.get("name"),
        "restaurant_url": restaurant.get("url"),
        "review_id": review_id,
        "commenter_id": reviewer_profile_url.split("/")[-1] if reviewer_profile_url else None,
        "commenter_name": commenter_name,
        "comment_text": comment_text,
        "comment_time": comment_time,
        "vote_count": vote_count,
        "rating": rating,
        "comment_images": comment_images,
        "source_url": restaurant.get("url"),
        "crawl_date": now_date_str(),
    }
    return doc

def extract_reviews_from_page(driver, restaurant) -> List[Dict]:
    items = find_all(driver, By.CSS_SELECTOR, ".review-item, .review, .comment-item")
    results = []
    for it in items:
        try:
            doc = parse_review_item(driver, it, restaurant)
            results.append(doc)
        except Exception:
            continue
    return results

def crawl_reviews_for_restaurant(driver, restaurant, max_pages: int = 10) -> List[Dict]:
    driver.get(restaurant["url"])
    time.sleep(2)

    open_review_tab_if_exists(driver)
    time.sleep(1)

    all_reviews = []
    for page_idx in range(max_pages):
        page_reviews = extract_reviews_from_page(driver, restaurant)
        all_reviews.extend(page_reviews)

        # Phân trang bên trong tab review
        next_btn = find_or_none(driver, By.CSS_SELECTOR, ".pagination a.next, a.next")
        if next_btn and next_btn.is_enabled():
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(1.2)
        else:
            break

    # Sắp xếp theo comment_time giảm dần
    def sort_key(x):
        return x["comment_time"] or ""
    all_reviews.sort(key=sort_key, reverse=True)
    return all_reviews

# ----------------------------- Cào user profile -----------------------------

def crawl_user_profile(driver, profile_url: str) -> Optional[Dict]:
    try:
        driver.get(profile_url)
        time.sleep(1.5)
    except WebDriverException:
        return None

    user_name = safe_text(find_or_none(driver, By.CSS_SELECTOR, ".profile-name, .user-name, h1"))
    join_date = None
    total_reviews = None

    # Ngày tham gia
    join_el = find_or_none(driver, By.CSS_SELECTOR, ".join-date, .member-since, .meta .date")
    if join_el:
        join_date = to_iso(safe_text(join_el))

    # Tổng số reviews
    tot_el = find_or_none(driver, By.CSS_SELECTOR, ".total-review, .review-count, .stats .reviews")
    if tot_el:
        try:
            total_reviews = int("".join(ch for ch in safe_text(tot_el) if ch.isdigit()))
        except Exception:
            total_reviews = None

    # Reviews gần đây
    recent_reviews = []
    items = find_all(driver, By.CSS_SELECTOR, ".review-item, .review, .comment-item")
    for it in items[:10]:
        rname = safe_text(find_or_none(it, By.CSS_SELECTOR, ".place-name, .restaurant-name, .title a"))
        rid = it.get_attribute("data-id") if it else None
        rt_el = find_or_none(it, By.CSS_SELECTOR, ".stars, .rating")
        rating = None
        if rt_el:
            dr = rt_el.get_attribute("data-rating")
            try:
                rating = float(dr) if dr else None
            except Exception:
                rating = None
        t_el = find_or_none(it, By.CSS_SELECTOR, ".time, .date")
        rtime = to_iso(safe_text(t_el)) if t_el else None

        recent_reviews.append({
            "review_id": rid,
            "restaurant_name": rname,
            "rating": rating,
            "time": rtime
        })

    doc = {
        "user_id": profile_url.split("/")[-1],
        "user_name": user_name,
        "join_date": join_date,
        "total_reviews": total_reviews,
        "profile_url": profile_url,
        "recent_reviews": recent_reviews,
        "crawl_date": now_date_str()
    }
    return doc

# ----------------------------- Lưu MongoDB -----------------------------

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

def upsert_user(doc: Dict):
    if not doc:
        return
    users_col.update_one({"user_id": doc["user_id"]}, {"$set": doc}, upsert=True)

# ----------------------------- Orchestrator -----------------------------

def crawl_from_restaurants(start_pages: int = 20, review_pages: int = 50, headless: bool = True):
    driver = setup_driver(headless=headless)
    try:
        restaurants = list_restaurants_hcm(driver, max_pages=start_pages)
        print(f"Found {len(restaurants)} restaurants")
        for idx, r in enumerate(restaurants):
            print(f"[{idx+1}/{len(restaurants)}] Crawling {r['name']}")
            reviews = crawl_reviews_for_restaurant(driver, r, max_pages=review_pages)
            upsert_reviews(reviews)

            # optional: crawl user profiles from extracted reviews
            for rv in reviews[:20]:
                if rv.get("commenter_id"):
                    profile_url = f"https://www.foody.vn/thanh-vien/{rv['commenter_id']}"
                    user_doc = crawl_user_profile(driver, profile_url)
                    upsert_user(user_doc)

            # throttle nhẹ
            time.sleep(1.0)
    finally:
        driver.quit()

def crawl_from_featured_dishes(start_pages: int = 2, review_pages: int = 5, headless: bool = True):
    driver = setup_driver(headless=headless)
    try:
        dishes = list_featured_dishes_hcm(driver, max_pages=start_pages)
        print(f"Found {len(dishes)} featured dish pages")
        for idx, d in enumerate(dishes):
            print(f"[{idx+1}/{len(dishes)}] Crawling {d['name']}")
            reviews = crawl_reviews_for_restaurant(driver, d, max_pages=review_pages)
            upsert_reviews(reviews)

            for rv in reviews[:20]:
                if rv.get("commenter_id"):
                    profile_url = f"https://www.foody.vn/thanh-vien/{rv['commenter_id']}"
                    user_doc = crawl_user_profile(driver, profile_url)
                    upsert_user(user_doc)

            time.sleep(1.0)
    finally:
        driver.quit()

if __name__ == "__main__":
    # Chọn một trong hai nguồn khởi đầu:
    # 1) Bắt đầu từ quán
    crawl_from_restaurants(start_pages=20, review_pages=50, headless=True)

    # 2) Hoặc bắt đầu từ món ăn đề cử
    #crawl_from_featured_dishes(start_pages=2, review_pages=10, headless=True)
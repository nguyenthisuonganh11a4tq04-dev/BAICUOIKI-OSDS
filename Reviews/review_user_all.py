# ================== 1. IMPORT ==================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import re
import hashlib
from urllib.parse import urljoin
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# ================== 2. CẤU HÌNH  ==================
IN_XLSX = "restaurants_all_districts_from_home_1.xlsx"   
IN_SHEET = "ALL"                                         

OUT_XLSX = "review_user_all.xlsx"                        

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "review_quan_db"                                 
MONGO_COL = "review_user_all"                            

TEST_LIMIT_RESTAURANTS = 0   # 0 = chạy hết quán
MAX_LOADMORE = 400           # số lần bấm "Xem thêm bình luận"
WAIT_GROW_SECONDS = 25       # chờ tăng số review sau mỗi lần bấm

# ================== 3. FIREFOX CONFIG ==================
gecko_path = r"C:/Users/User/OneDrive/Desktop/Ma Nguon Mo/DO AN CUOI KY/geckodriver.exe"
service = Service(gecko_path)

options = webdriver.firefox.options.Options()
options.binary_location = r"C:/Program Files/Mozilla Firefox/firefox.exe"
options.headless = False

driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, 25)

# ================== 4. HÀM PHỤ ==================
def js_click(el):
    driver.execute_script("arguments[0].click();", el)

def safe_sheet_name(name: str) -> str:
    if not name:
        return "Unknown"
    name = str(name).strip()
    name = re.sub(r'[:\\/?*\[\]]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:31] if name else "Unknown"

def to_comment_url(base_url: str) -> str:
    u = (base_url or "").strip()
    if not u:
        return u
    if "/binh-luan" in u:
        return u
    return u.rstrip("/") + "/binh-luan"

def get_review_count() -> int:
    return len(driver.find_elements(By.CSS_SELECTOR, "li.review-item"))

def load_all_reviews():
    last = get_review_count()
    clicks = 0

    while clicks < MAX_LOADMORE:
        btns = driver.find_elements(By.CSS_SELECTOR, "div.pn-loadmore a.fd-btn-more")
        if not btns:
            break

        btn = btns[0]
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.8)
            js_click(btn)
            clicks += 1
        except:
            break

        start = time.time()
        grown = False
        while time.time() - start < WAIT_GROW_SECONDS:
            now = get_review_count()
            if now > last:
                last = now
                grown = True
                break
            time.sleep(1)

        if not grown:
            break

def norm_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pick_attr(el, attrs):
    for a in attrs:
        v = el.get_attribute(a)
        if v:
            return v
    return ""

def make_hash_id(*parts) -> str:
    raw = "||".join([norm_text(p) for p in parts if p is not None])
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()

def parse_one_review(li, restaurant_url):
    # review_id để chống trùng 
    review_id = ""
    try:
        rp = li.find_element(By.CSS_SELECTOR, "div.review-points")
        review_id = pick_attr(rp, ["data-review"])  # ví dụ: review_3976939
    except:
        review_id = ""
    # user_name
    user_name = ""
    try:
        user_name = li.find_element(By.CSS_SELECTOR, "a.ru-username").text
    except:
        user_name = ""
    # user_rating
    user_rating = None
    try:
        t = li.find_element(By.CSS_SELECTOR, "div.review-points span.ng-binding").text
        t = norm_text(t).replace(",", ".")
        user_rating = float(t) if t else None
    except:
        user_rating = None
    # review_time
    review_time = None
    try:
        rt = li.find_element(By.CSS_SELECTOR, "span.ru-time")
        review_time = rt.get_attribute("title") or rt.text
        review_time = norm_text(review_time)
        if not review_time:
            review_time = None
    except:
        review_time = None
    # review_text
    review_text = ""
    try:
        review_text = li.find_element(By.CSS_SELECTOR, "div.review-des").text
    except:
        review_text = ""
    review_text = norm_text(review_text)
    # media_urls: lưu URL của  ảnh & video 
    media = []
    # ảnh
    try:
        imgs = li.find_elements(By.CSS_SELECTOR, "ul.review-photos img")
        for im in imgs:
            src = pick_attr(im, ["data-original", "data-src", "src"])
            src = (src or "").strip()
            if src:
                media.append(src)
    except:
        pass
    # video
    try:
        vids = li.find_elements(By.CSS_SELECTOR, "a.foody-video")
        for v in vids:
            u = (v.get_attribute("data-video-url") or "").strip()
            if u:
                u = urljoin(restaurant_url, u)
                media.append(u)
    except:
        pass

    media_urls = "|".join(list(dict.fromkeys(media)))  # mỗi link cách nhau dấu |

    # fallback review_id nếu thiếu
    if not review_id:
        review_id = "hash_" + make_hash_id(restaurant_url, user_name, str(user_rating), review_time or "", review_text)

    return {
        "review_id": review_id,
        "user_name": norm_text(user_name),
        "user_rating": user_rating,
        "review_text": review_text,
        "media_urls": media_urls,
        "review_time": review_time
    }

# ================== 5. ĐỌC LIST QUÁN  ==================
df_in = pd.read_excel(IN_XLSX, sheet_name=IN_SHEET)
need_cols = ["restaurant_url", "restaurant_name", "district"]
for c in need_cols:
    if c not in df_in.columns:
        raise ValueError(f"Thiếu cột {c} trong {IN_XLSX} / sheet {IN_SHEET}")

df_in["restaurant_url"] = df_in["restaurant_url"].astype(str).str.strip()
df_in["restaurant_name"] = df_in["restaurant_name"].astype(str).str.strip()
df_in["district"] = df_in["district"].astype(str).str.strip()

if TEST_LIMIT_RESTAURANTS and TEST_LIMIT_RESTAURANTS > 0:
    df_in = df_in.head(TEST_LIMIT_RESTAURANTS).copy()

print(f" Đọc {len(df_in)} quán từ {IN_XLSX} / sheet {IN_SHEET}")

# ================== 6. KẾT NỐI MONGODB ==================
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
col = db[MONGO_COL]
col.create_index("review_id", unique=True)
print(" Đã kết nối MongoDB:", MONGO_DB, "/", MONGO_COL)

# ================== 7. CÀO REVIEW_USER ==================
total_new = 0
total_upd = 0
total_skip = 0

for idx, row in df_in.iterrows():
    base_url = row["restaurant_url"]
    restaurant_name = row["restaurant_name"]
    district = row["district"]

    if not base_url or base_url.lower() == "nan":
        total_skip += 1
        continue

    comment_url = to_comment_url(base_url)

    try:
        driver.get(comment_url)
        time.sleep(2)

        # chờ có review list 
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.review-list, li.review-item")))
        except TimeoutException:
            # có thể quán không có bình luận
            continue
        # load thêm đến khi hết
        load_all_reviews()
        time.sleep(1)

        lis = driver.find_elements(By.CSS_SELECTOR, "li.review-item")
        if not lis:
            continue

        for li in lis:
            data = parse_one_review(li, comment_url)

            doc = {
                "review_id": data["review_id"],
                "restaurant_url": base_url,
                "restaurant_name": restaurant_name,
                "district": district,
                "user_name": data["user_name"],
                "user_rating": data["user_rating"],
                "review_text": data["review_text"],
                "media_urls": data["media_urls"],
                "review_time": data["review_time"],
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "foody.vn"
            }

            res = col.update_one({"review_id": doc["review_id"]}, {"$set": doc}, upsert=True)
            if res.upserted_id is not None:
                total_new += 1
            else:
                if res.matched_count > 0:
                    total_upd += 1

        print(f"[{idx+1}/{len(df_in)}]  {district} | {restaurant_name} | reviews={len(lis)}")

    except Exception as e:
        total_skip += 1
        print(f"[{idx+1}/{len(df_in)}]  Lỗi: {e}")


print("========== TỔNG KẾT ==========")
print(" Insert mới:", total_new)
print(" Update:", total_upd)
print(" Lỗi:", total_skip)

# ================== 8. EXPORT FILE XLSX: ==================
docs = list(col.find({}, {"_id": 0, "review_id": 0, "source": 0}))  
df_out = pd.DataFrame(docs)
if df_out.empty:
    print(" Chưa có dữ liệu để export.")
else:
    order_cols = [
        "restaurant_url", "restaurant_name", "district",
        "user_name", "user_rating", "review_text",
        "media_urls", "review_time", "scraped_at"
    ]
    for c in order_cols:
        if c not in df_out.columns:
            df_out[c] = None
    df_out = df_out[order_cols]

    df_out = df_out.sort_values(["district", "restaurant_name", "scraped_at"], na_position="last").reset_index(drop=True)

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        df_out.to_excel(writer, sheet_name="ALL", index=False)

        used = set(["ALL"])
        for d, g in df_out.groupby("district", dropna=False):
            sheet = safe_sheet_name(d)
            base = sheet
            i = 2
            while sheet in used:
                suffix = f"_{i}"
                sheet = (base[:31-len(suffix)] + suffix) if len(base) + len(suffix) > 31 else base + suffix
                i += 1
            used.add(sheet)
            g.to_excel(writer, sheet_name=sheet, index=False)
    print(f" Đã xuất Excel: {OUT_XLSX}")


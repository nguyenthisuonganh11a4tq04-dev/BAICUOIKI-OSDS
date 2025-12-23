# ================== 1. IMPORT ==================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import getpass
import re
from urllib.parse import urljoin
import pandas as pd
from pymongo import MongoClient


# ================== 2. FIREFOX CONFIG ==================
gecko_path = r"C:/Users/User/OneDrive/Desktop/Ma Nguon Mo/DO AN CUOI KY/geckodriver.exe"
service = Service(gecko_path)

options = webdriver.firefox.options.Options()
options.binary_location = r"C:/Program Files/Mozilla Firefox/firefox.exe"
options.headless = False

driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, 25)

# ================== 3.  LINK LOGIN ==================
LOGIN_URL = "https://id.foody.vn/account/login?returnUrl=https://www.foody.vn/"

# ================== 4. HELPER FUNCTIONS ==================
def js_click(el):
    driver.execute_script("arguments[0].click();", el)

def dismiss_login_popup_if_any():
    try:
        popup_title = driver.find_elements(By.XPATH, "//*[contains(text(),'Đăng nhập hệ thống')]")
        if popup_title:
            btns = driver.find_elements(By.XPATH, "//button[contains(.,'Hủy')] | //a[contains(.,'Hủy')]")
            if btns:
                js_click(btns[0])
                time.sleep(1)
    except:
        pass

def safe_sheet_name(name: str) -> str:
    """
    Sheet name: <=31 ký tự, không chứa : \ / ? * [ ]
    """
    if not name:
        return "Unknown"
    bad = [":", "\\", "/", "?", "*", "[", "]"]
    for b in bad:
        name = name.replace(b, " ")
    name = " ".join(name.split()).strip()
    if not name:
        return "Unknown"
    return name[:31]

def normalize_area_text(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s

def parse_district(address: str) -> str:
    """
    Tách khu vực từ địa chỉ:
    - Quận + số (Quận 1..12...)
    - Quận + chữ (Quận Tân Phú, Quận Bình Thạnh...)
    - Huyện + chữ (Huyện Củ Chi...)
    - TP. Thủ Đức (hoặc Thủ Đức)
    Nếu không thấy -> Unknown
    """
    if not address:
        return "Unknown"

    addr = normalize_area_text(address)

    # Tách theo dấu phẩy -> thường có mảnh "Quận ...", "Huyện ..."
    parts = [normalize_area_text(p) for p in addr.split(",") if normalize_area_text(p)]

    # Ưu tiên mảnh bắt đầu bằng Quận/Huyện
    for p in parts:
        if re.match(r"^(Quận)\s+", p, flags=re.IGNORECASE):
            # Chuẩn hóa "Quận ..."
            tail = p[4:].strip()  # bỏ chữ Quận
            return "Quận " + tail
        if re.match(r"^(Huyện)\s+", p, flags=re.IGNORECASE):
            tail = p[5:].strip()  # bỏ chữ Huyện
            return "Huyện " + tail

    # Thủ Đức (có thể đứng riêng hoặc kèm TP.)
    if re.search(r"\bthủ\s*đức\b", addr, flags=re.IGNORECASE):
        return "TP. Thủ Đức"

    # Fallback kiểu Q.1 / Q1 / Q. Tan Phu
    for p in parts:
        m = re.match(r"^Q\.?\s*(.+)$", p, flags=re.IGNORECASE)
        if m:
            tail = normalize_area_text(m.group(1))
            # nếu là số -> Quận <số>, nếu là chữ -> Quận <chữ>
            return "Quận " + tail

    return "Unknown"

def get_restaurant_items():
    """
    Trả về list dict {restaurant_url, restaurant_name, address, district}
    Lấy theo DOM card kiểu "content-item"
    """
    items = []
    cards = driver.find_elements(By.CSS_SELECTOR, "div.content-item")
    for card in cards:
        try:
            # link + name thường nằm ở div.title a
            try:
                a = card.find_element(By.CSS_SELECTOR, "div.title a")
            except:
                a = card.find_element(By.CSS_SELECTOR, "a.ng-binding")

            href = a.get_attribute("href") or ""
            if href.startswith("/"):
                href = urljoin(driver.current_url, href)

            name = (a.text or "").strip()

            addr = ""
            try:
                addr = card.find_element(By.CSS_SELECTOR, "div.desc").text.strip()
            except:
                addr = ""

            if not href:
                continue

            district = parse_district(addr)

            items.append({
                "restaurant_url": href.strip(),
                "restaurant_name": name,
                "address": addr,
                "district": district,
                "source": "foody.vn"
            })
        except:
            continue
    return items

def count_unique_urls():
    urls = set()
    for it in get_restaurant_items():
        if it.get("restaurant_url"):
            urls.add(it["restaurant_url"])
    return len(urls)

# ================== 5. MỞ LOGIN & ĐĂNG NHẬP ==================
driver.get(LOGIN_URL)
time.sleep(3)

email = input("Nhập Email Foody: ").strip()
password = getpass.getpass("Nhập mật khẩu Foody: ")

email_box = wait.until(EC.presence_of_element_located((By.ID, "Email")))
email_box.clear()
email_box.send_keys(email)

pass_box = wait.until(EC.presence_of_element_located((By.ID, "Password")))
pass_box.clear()
pass_box.send_keys(password)

login_btn = wait.until(EC.element_to_be_clickable((By.ID, "bt_submit")))
js_click(login_btn)

# ====== CHỜ REDIRECT VỀ FOODY ======
try:
    wait.until(lambda d: ("foody.vn" in d.current_url) and ("id.foody.vn" not in d.current_url))
except TimeoutException:
    print(" Không thấy redirect rõ ràng về foody.vn (mạng yếu hoặc login chưa hoàn tất).")
    print("URL hiện tại:", driver.current_url)

time.sleep(2)
print(" Đang ở trang:", driver.current_url)


# ================== 6. KẾT NỐI MONGODB ==================
client = MongoClient("mongodb://localhost:27017/")
db = client["foody_db"]
col = db["restaurants_all"]

try:
    col.create_index("restaurant_url", unique=True)
except:
    pass
print(" Đã kết nối MongoDB")

# ================== 7. CLICK 'XEM THÊM' ĐẾN KHI HẾT ==================
MAX_CLICK = 500
WAIT_GROW_TIMEOUT = 50

last_unique = count_unique_urls()
print(f" Bắt đầu với {last_unique} card(unique url)")

click_count = 0
while click_count < MAX_CLICK:
    dismiss_login_popup_if_any()

    btn = None
    selectors = [
        "a.fd-btn-more",
        "#scrollLoadingPage a",
        "#scrollLoadingPage",
        "a[rel='next']",
    ]

    for sel in selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            btn = els[0]
            break

    if not btn:
        print(" Không còn nút 'Xem thêm' → DỪNG")
        break

    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
        time.sleep(1)
        js_click(btn)
        click_count += 1
        print(f"Click Xem thêm: {click_count}")
    except:
        print(" Click nút 'Xem thêm' lỗi → DỪNG")
        break

    start = time.time()
    grown = False
    while time.time() - start < WAIT_GROW_TIMEOUT:
        current_unique = count_unique_urls()
        if current_unique > last_unique:
            print(f"   Tăng: {last_unique} → {current_unique}")
            last_unique = current_unique
            grown = True
            break
        time.sleep(1)

    if not grown:
        print(" Không tăng sau khi chờ đủ -> DỪNG")
        break

print(f" KẾT THÚC LOAD: tổng card(unique url) ≈ {last_unique}")


# ================== 8. THU THẬP + LƯU MONGO  ==================
all_items = get_restaurant_items()
print(f"Tổng số card DOM: {len(all_items)}")

inserted = 0
updated = 0
skipped = 0

for it in all_items:
    try:
        url = (it.get("restaurant_url") or "").strip()
        if not url:
            skipped += 1
            continue

        # upsert: có thì update, chưa có thì insert
        res = col.update_one(
            {"restaurant_url": url},
            {"$set": {
                "district": it.get("district", "Unknown"),
                "restaurant_name": it.get("restaurant_name", ""),
                "address": it.get("address", ""),
                "restaurant_url": url,
                "source": it.get("source", "foody.vn")
            }},
            upsert=True
        )

        if res.upserted_id is not None:
            inserted += 1
        else:
            if res.matched_count > 0:
                updated += 1

    except:
        skipped += 1

print(f" Insert mới: {inserted}")
print(f" Update (đã có url): {updated}")
print(f" Bỏ qua (thiếu url/lỗi): {skipped}")

# ================== 9. EXPORT EXCEL: ALL + MỖI KHU VỰC 1 SHEET ==================
docs = list(col.find({}, {"_id": 0}))
df = pd.DataFrame(docs)

if not df.empty:
    # TÍNH LẠI district TỪ address để sửa Unknown do dữ liệu cũ
    if "address" in df.columns:
        df["district"] = df["address"].apply(parse_district)

    # sắp xếp dễ nhìn
    df = df.sort_values(["district", "restaurant_name"], na_position="last").reset_index(drop=True)

output_file = "restaurants_all_districts_from_home_1.xlsx"

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="ALL", index=False)

    if not df.empty and "district" in df.columns:
        for district, g in df.groupby("district"):
            sheet = safe_sheet_name(str(district))
            g.to_excel(writer, sheet_name=sheet, index=False)

print(f" Đã export {len(df)} dòng ra file {output_file}")

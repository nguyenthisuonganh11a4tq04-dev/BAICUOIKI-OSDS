# ================== 1. IMPORT ==================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
import time
import getpass
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# ================== 2. FIREFOX CONFIG ==================
gecko_path = r"C:/Users/User/OneDrive/Desktop/Ma Nguon Mo/DO AN CUOI KY/geckodriver.exe"
service = Service(gecko_path)

options = webdriver.firefox.options.Options()
options.binary_location = r"C:/Program Files/Mozilla Firefox/firefox.exe"
options.headless = False

driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, 25)
actions = ActionChains(driver)


# ================== 3. MỞ FOODY ==================
driver.get("https://id.foody.vn/account/login?returnUrl=https://www.foody.vn/")
time.sleep(5)

email = input("Nhập Email Foody: ").strip()
password = getpass.getpass("Nhập mật khẩu Foody: ")

# --- TÌM Ô EMAIL ---
email_box = wait.until(
    EC.presence_of_element_located((By.ID, "Email"))
)
email_box.clear()
email_box.send_keys(email)

# --- TÌM Ô PASSWORD ---
pass_box = wait.until(
    EC.presence_of_element_located((By.ID, "Password"))
)
pass_box.clear()
pass_box.send_keys(password)

time.sleep(1)

# --- NÚT ĐĂNG NHẬP ---
login_btn = wait.until(
    EC.element_to_be_clickable((By.ID, "bt_submit"))
)
login_btn.click()
time.sleep(5)
print(" Đã xác nhận đăng nhập xong")

# ================== KẾT NỐI MONGODB ==================
client = MongoClient("mongodb://localhost:27017/")
db = client["foody_db"]
col_restaurants = db["restaurants"]

print(" Đã kết nối MongoDB")

# ================== 4. MỞ BỘ LỌC ==================
bo_loc = wait.until(
    EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "a.ico-nofilter, a.ico-filter")
    )
)
driver.execute_script("arguments[0].click();", bo_loc)
time.sleep(3)
print(" Đã mở Bộ lọc")

# ================== 5. CHỌN QUẬN 1 ==================
label_quan1 = wait.until(
    EC.element_to_be_clickable(
        (By.XPATH, "//label[contains(text(),'Quận 1')]")
    )
)
driver.execute_script("arguments[0].click();", label_quan1)
time.sleep(1)
print(" Đã chọn Quận 1")

# ================== 6. NHẤN TÌM KIẾM ==================
btn_search = wait.until(
    EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "a.fd-btn.blue")
    )
)
driver.execute_script("arguments[0].click();", btn_search)
time.sleep(5)
print(" Đã chuyển sang trang kết quả Quận 1")

# ================== 7. LOAD HẾT QUÁN (CHỐNG MẠNG YẾU) ==================
MAX_CLICK = 150          # giới hạn an toàn 
WAIT_GROW_TIMEOUT = 20   # chờ DOM tăng tối đa 20s
click_count = 0

def get_count():
    return len(driver.find_elements(By.CSS_SELECTOR, "a[data-bind*='BranchUrl']"))

last_count = get_count()
print(f" Bắt đầu với {last_count} quán")

while click_count < MAX_CLICK:
    try:
        btn_more = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#scrollLoadingPage, a.next, a.btn-load-more")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", btn_more)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", btn_more)
        click_count += 1
        print(f" Click {click_count}")

        #  CHỜ DOM TĂNG
        start = time.time()
        while time.time() - start < WAIT_GROW_TIMEOUT:
            current_count = get_count()
            if current_count > last_count:
                print(f"  Tăng từ {last_count} → {current_count}")
                last_count = current_count
                break
            time.sleep(1)
        else:
            print(" Không tăng sau khi chờ đủ → DỪNG")
            break
    except:
        print(" Không còn nút load → DỪNG")
        break
print(f" KẾT THÚC: {last_count} quán")

# ================== 8. LƯU LINK QUÁN VÀO MONGO ==================
restaurant_links = set()
inserted_count = 0

cards = driver.find_elements(By.CSS_SELECTOR, "a[data-bind*='BranchUrl']")
for c in cards:
    link = c.get_attribute("href")
    if not link:
        continue
    restaurant_links.add(link)
    # kiểm tra trùng trong Mongo
    if col_restaurants.find_one({"restaurant_url": link}):
        continue

    doc = {
        "district": "Quận 1",
        "restaurant_url": link,
        "crawl_time": datetime.now(),
        "crawler_email": email,
        "source": "foody.vn"
    }

    col_restaurants.insert_one(doc)
    inserted_count += 1

print(f" Đã lưu {inserted_count} quán mới vào MongoDB")
print(f" Tổng số link quán Quận 1 (unique): {len(restaurant_links)}")

# ================== 12. EXPORT EXCEL TỪ MONGO ==================
docs = list(col_restaurants.find({"district": "Quận 1"}, {"_id": 0}))
df = pd.DataFrame(docs)
output_file = "restaurants_quan1.xlsx"
df.to_excel(output_file, index=False)

print(f" Đã export {len(df)} dòng ra file {output_file}")

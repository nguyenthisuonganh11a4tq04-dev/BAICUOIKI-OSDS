# ================== 1. IMPORT ==================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from pymongo import MongoClient
import pandas as pd
import os, re, time, random
from datetime import datetime

# ================== 2. CONFIG ==================
IN_XLSX  = r"restaurants_all_districts_from_home_1.xlsx"   
OUT_XLSX = r"review_quan_restaurants_all.xlsx"            

GECKO_PATH = r"C:/Users/User/OneDrive/Desktop/Ma Nguon Mo/DO AN CUOI KY/geckodriver.exe"
FIREFOX_BINARY = r"C:/Program Files/Mozilla Firefox/firefox.exe"
HEADLESS = False
WAIT_SEC = 25
SLEEP_MIN = 0.8
SLEEP_MAX = 1.5

# ================== 3. KẾT NỐI MONGODB ==================
client = MongoClient("mongodb://localhost:27017/")
db = client["review_quan_db"]
col = db["review_restaurants_all"]
try:
    col.create_index("restaurant_url", unique=True)
except:
    pass
print(" Đã kết nối MongoDB")

# ================== 4. HELPER ==================
def safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", ".")
        m = re.search(r"(\d+(\.\d+)?)", s)
        return float(m.group(1)) if m else None
    except:
        return None

def safe_sheet_name(name: str) -> str:
    if not name:
        return "Unknown"
    name = str(name).strip()
    name = re.sub(r'[:\\/?*\[\]]', ' ', name)
    name = " ".join(name.split()).strip()
    return name[:31] if name else "Unknown"

def tiny_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

def scrape_the_loai_quan(driver):
    parts = []
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "div.category div.category-items a")
        for e in els:
            t = (e.text or "").strip()
            if t:
                parts.append(t)
    except:
        pass

    try:
        els = driver.find_elements(By.CSS_SELECTOR, "div.category div.category-cuisines a")
        for e in els:
            t = (e.text or "").strip()
            if t:
                parts.append(t)
    except:
        pass

    if not parts:
        try:
            box = driver.find_element(By.CSS_SELECTOR, "div.category")
            t = re.sub(r"\s+", " ", (box.text or "").strip())
            if t:
                parts.append(t)
        except:
            pass

    seen = set()
    uniq = []
    for p in parts:
        if p not in seen:
            uniq.append(p)
            seen.add(p)

    return " - ".join(uniq) if uniq else None

def scrape_scores(driver):
    result = {
        "tieu_chi_1_vi_tri": None,
        "tieu_chi_2_gia_ca": None,
        "tieu_chi_3_chat_luong": None,
        "tieu_chi_4_phuc_vu": None,
        "tieu_chi_5_khong_gian": None,
        "diem_tb_tieu_chi": None
    }

    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "div.micro-home-point div.micro-home-static table tbody tr")
        for r in rows:
            tds = r.find_elements(By.TAG_NAME, "td")
            if len(tds) < 2:
                continue

            label = (tds[0].text or "").strip().lower()

            val = None
            try:
                b = tds[-1].find_element(By.TAG_NAME, "b")
                val = safe_float(b.text)
            except:
                val = safe_float(tds[-1].text)

            if "vị trí" in label or "vi tri" in label:
                result["tieu_chi_1_vi_tri"] = val
            elif "giá cả" in label or "gia ca" in label:
                result["tieu_chi_2_gia_ca"] = val
            elif "chất lượng" in label or "chat luong" in label:
                result["tieu_chi_3_chat_luong"] = val
            elif "phục vụ" in label or "phuc vu" in label:
                result["tieu_chi_4_phuc_vu"] = val
            elif "không gian" in label or "khong gian" in label:
                result["tieu_chi_5_khong_gian"] = val
    except:
        pass

    scores = [
        result["tieu_chi_1_vi_tri"],
        result["tieu_chi_2_gia_ca"],
        result["tieu_chi_3_chat_luong"],
        result["tieu_chi_4_phuc_vu"],
        result["tieu_chi_5_khong_gian"],
    ]
    if all(x is not None for x in scores):
        result["diem_tb_tieu_chi"] = round(sum(scores) / 5, 2)

    return result

# ================== 5. ĐỌC FILE LINK ==================
df_in = pd.read_excel(IN_XLSX, sheet_name="ALL")
need_cols = ["restaurant_url", "restaurant_name", "address", "district"]
for c in need_cols:
    if c not in df_in.columns:
        raise ValueError(f"Thiếu cột '{c}' trong sheet ALL")

df_in["restaurant_url"] = df_in["restaurant_url"].astype(str).str.strip()
df_in = df_in[df_in["restaurant_url"].str.startswith("http")].reset_index(drop=True)
print(f" Input: {len(df_in)} link")

# ================== 6. FIREFOX CONFIG ==================
service = Service(GECKO_PATH)
options = webdriver.firefox.options.Options()
options.binary_location = FIREFOX_BINARY
options.headless = HEADLESS

driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, WAIT_SEC)


# ================== 7. CÀO + LƯU MONGO ==================
for idx, row in df_in.iterrows():
    url = str(row["restaurant_url"]).strip()
    name = str(row["restaurant_name"]).strip()
    address = str(row["address"]).strip()
    district = str(row["district"]).strip()

    existed = col.find_one({"restaurant_url": url}, {"_id": 0})
    if existed:
        ok = (
            existed.get("the_loai_quan") is not None and
            existed.get("tieu_chi_1_vi_tri") is not None and
            existed.get("tieu_chi_2_gia_ca") is not None and
            existed.get("tieu_chi_3_chat_luong") is not None and
            existed.get("tieu_chi_4_phuc_vu") is not None and
            existed.get("tieu_chi_5_khong_gian") is not None and
            existed.get("diem_tb_tieu_chi") is not None
        )
        if ok:
            print(f"[{idx+1}/{len(df_in)}]  Skip (đã có): {name}")
            continue

    print(f"[{idx+1}/{len(df_in)}]  {name}")
    rec = {
        "restaurant_url": url,
        "restaurant_name": name,
        "address": address,
        "district": district,                 
        "the_loai_quan": None,
        "tieu_chi_1_vi_tri": None,
        "tieu_chi_2_gia_ca": None,
        "tieu_chi_3_chat_luong": None,
        "tieu_chi_4_phuc_vu": None,
        "tieu_chi_5_khong_gian": None,
        "diem_tb_tieu_chi": None,
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        #vùng category xuất hiện
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.category")))
        except TimeoutException:
            pass
          
        # để table tiêu chí xuất hiện
        driver.execute_script("window.scrollBy(0, 900);")
        tiny_sleep()

        rec["the_loai_quan"] = scrape_the_loai_quan(driver)
        rec.update(scrape_scores(driver))

    except Exception as e:
        print("  Lỗi:", str(e)[:120])

    col.update_one(
        {"restaurant_url": url},
        {"$set": rec},
        upsert=True
    )

    tiny_sleep()
driver.quit()
print(" Đã cào xong, bắt đầu export Excel...")

# ================== 8. EXPORT EXCEL ==================
docs = list(col.find({}, {"_id": 0}))
df = pd.DataFrame(docs)
if df.empty:
    raise ValueError("MongoDB chưa có dữ liệu để export.")

if "district" in df.columns and "restaurant_name" in df.columns:
    df = df.sort_values(["district", "restaurant_name"], na_position="last").reset_index(drop=True)

OUTPUT_COLS = [
    "restaurant_url",
    "restaurant_name",
    "address",
    "the_loai_quan",
    "tieu_chi_1_vi_tri",
    "tieu_chi_2_gia_ca",
    "tieu_chi_3_chat_luong",
    "tieu_chi_4_phuc_vu",
    "tieu_chi_5_khong_gian",
    "diem_tb_tieu_chi",
    "scraped_at",
]

with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    df[OUTPUT_COLS].to_excel(writer, sheet_name="ALL", index=False)

    if "district" in df.columns:
        for district, g in df.groupby("district", dropna=False):
            sheet = safe_sheet_name(district)
            g[OUTPUT_COLS].to_excel(writer, sheet_name=sheet, index=False)

print(f" Đã export ra file: {OUT_XLSX}")


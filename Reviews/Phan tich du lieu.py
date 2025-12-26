# Import thư viện pymongo
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import os
import math


OUTPUT_DIR = r"C:\Users\User\OneDrive\Desktop\Ma Nguon Mo\DO AN CUOI KY\mongoDB-test"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Bước 1: Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/') # ket noi den server mongoDB

# Bước 2: Chọn DB vv Tạo Collection 
db = client["review_quan_db"]
b2 = db["review_restaurants_all"]  # Bảng 2: review quán
b3 = db["review_user_all"]         # Bảng 3: review user

print(" ==================Connected MongoDB==================")


# Bước 3: COUNTS
print("Bảng 2 (review_restaurants_all):", b2.count_documents({}))
print("Bảng 3 (review_user_all):", b3.count_documents({}))


# Bước 4: TRUY VẤN
# Q1: Đếm số review theo từng quán
pipeline = [
    {
        "$group": {
            "_id": "$restaurant_url",
            "restaurant_name": {"$first": "$restaurant_name"},
            "review_count": {"$sum": 1}
        }
    },
    {"$sort": {"review_count": -1}}
]

data = list(b3.aggregate(pipeline))
df = pd.DataFrame(data)

df.to_csv(f"{OUTPUT_DIR}/Q1_restaurant_review_count.csv", index=False, encoding="utf-8-sig")
print(" ==================Đã xuất thành công file==================")

# Q2: Missing values
fields = ["user_rating", "review_text", "media_urls", "review_time"]

rows = []
total = b3.count_documents({})

for f in fields:
    missing = b3.count_documents({"$or": [
    {f: {"$in": [None, ""]}},
    {f: {"$exists": False}}
]})
    rows.append({
        "field": f,
        "missing_count": missing,
        "missing_ratio": round(missing / total, 4)
    })

df = pd.DataFrame(rows)
df.to_csv(f"{OUTPUT_DIR}/Q2_missing_report.csv", index=False, encoding="utf-8-sig")
print("==================Đã xuất thành công file Q2==================")

# Q3: Review trùng lặp
pipeline = [
    {"$group": {"_id": "$review_id", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]

data = list(b3.aggregate(pipeline))
df = pd.DataFrame(data)

df.to_csv(f"{OUTPUT_DIR}/Q3_duplicate_reviews.csv", index=False, encoding="utf-8-sig")
print(" Xuất Q3_duplicate_reviews.csv")

# Q4: Phân phối user_rating
pipeline = [
    {"$group": {"_id": "$user_rating", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]

data = list(b3.aggregate(pipeline))
df = pd.DataFrame(data).rename(columns={"_id": "user_rating"})

df.to_csv(f"{OUTPUT_DIR}/Q4_rating_distribution.csv", index=False, encoding="utf-8-sig")
print(" Xuất Q4_rating_distribution.csv")

# Q5 : Rating bias - bỏ user_rating missing trước khi phân nhóm
pipeline = [
    # 1) Convert user_rating -> số (double). Nếu lỗi/Null => None
    {"$project": {
        "rating_num": {
            "$convert": {
                "input": "$user_rating",
                "to": "double",
                "onError": None,
                "onNull": None
            }
        }
    }},

    # 2) Lọc bỏ missing / convert lỗi
    {"$match": {"rating_num": {"$ne": None}}},

    # 3) Phân nhóm Thấp / Trung bình / Cao
    {"$project": {
        "rating_group": {
            "$cond": [
                {"$lt": ["$rating_num", 4]}, "Thấp",
                {"$cond": [
                    {"$lt": ["$rating_num", 8]}, "Trung bình", "Cao"
                ]}
            ]
        }
    }},

    # 4) Đếm số lượng mỗi nhóm
    {"$group": {"_id": "$rating_group", "count": {"$sum": 1}}},

    # (Tuỳ chọn) sắp xếp cho đẹp: Cao -> Trung bình -> Thấp
    {"$addFields": {
        "order": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$_id", "Cao"]}, "then": 1},
                    {"case": {"$eq": ["$_id", "Trung bình"]}, "then": 2},
                    {"case": {"$eq": ["$_id", "Thấp"]}, "then": 3}
                ],
                "default": 99
            }
        }
    }},
    {"$sort": {"order": 1}},
    {"$project": {"order": 0}}
]

data = list(b3.aggregate(pipeline))
df = pd.DataFrame(data).rename(columns={"_id": "rating_group"})

df.to_csv(os.path.join(OUTPUT_DIR, "Q5_rating_bias.csv"), index=False, encoding="utf-8-sig")
print(" Đã xuất Q5_rating_bias.csv")


# Q6 : Thể loại quán nào (full chuỗi the_loai_quan) có diem_tb_tieu_chi > 8.0?
pipeline = [
    {"$match": {"diem_tb_tieu_chi": {"$gt": 8.0}, "the_loai_quan": {"$nin": [None, ""]}}}
,

    # giữ nguyên full chuỗi the_loai_quan
    {"$group": {
        "_id": "$the_loai_quan",
        "restaurant_count": {"$sum": 1}
    }},
    {"$sort": {"restaurant_count": -1}}
]

df = pd.DataFrame(list(b2.aggregate(pipeline))).rename(columns={"_id": "the_loai_quan"})

df.to_csv(os.path.join(OUTPUT_DIR, "Q6_full_category_over_8.csv"),
          index=False, encoding="utf-8-sig")

print(" Q6_full_category_over_8.csv")


# Q7: Top 10 quán theo từng tiêu chí 
criteria_map = {
    "vi_tri": "tieu_chi_1_vi_tri",
    "gia_ca": "tieu_chi_2_gia_ca",
    "chat_luong": "tieu_chi_3_chat_luong",
    "phuc_vu": "tieu_chi_4_phuc_vu",
    "khong_gian": "tieu_chi_5_khong_gian"
}

for short_name, field in criteria_map.items():
    pipeline = [
        {"$match": {field: {"$ne": None}}},
        {"$sort": {field: -1}},
        {"$limit": 100},
        {"$project": {
            "_id": 0,
            "restaurant_url": 1,
            "restaurant_name": 1,
            "district": 1,
            "the_loai_quan": 1,
            field: 1
        }}
    ]

    df = pd.DataFrame(list(b2.aggregate(pipeline)))
    out_name = f"Q7_top_{short_name}.csv"
    df.to_csv(os.path.join(OUTPUT_DIR, out_name), index=False, encoding="utf-8-sig")
    print(f" {out_name}")


# Q8: Quán điểm cao nhưng ít review (nguy cơ “ảo điểm”)

pipeline = [
    {"$match": {"user_rating": {"$ne": None}}},
    {"$group": {
        "_id": "$restaurant_url",
        "restaurant_name": {"$first": "$restaurant_name"},
        "avg_rating": {"$avg": "$user_rating"},
        "review_count": {"$sum": 1}
    }},
    {"$match": {"avg_rating": {"$gte": 9}, "review_count": {"$lte": 5}}},
    {"$sort": {"avg_rating": -1}}
]
df = pd.DataFrame(list(b3.aggregate(pipeline))).rename(columns={"_id": "restaurant_url"})
df.to_csv(os.path.join(OUTPUT_DIR, "Q8_high_rating_low_review.csv"), index=False, encoding="utf-8-sig")
print(" Q8_high_rating_low_review.csv")

# Q9: diem_tb_tieu_chi (B2) có liên quan user_rating_mean (B3) không?


pipeline = [
    {"$match": {"user_rating": {"$ne": None}}},
    {"$lookup": {
        "from": "review_restaurants_all",
        "localField": "restaurant_url",
        "foreignField": "restaurant_url",
        "as": "r"
    }},
    {"$unwind": "$r"},
    {"$match": {"r.diem_tb_tieu_chi": {"$ne": None}}},
    {"$group": {
        "_id": "$restaurant_url",
        "restaurant_name": {"$first": "$restaurant_name"},
        "district": {"$first": "$district"},
        "user_rating_mean": {"$avg": "$user_rating"},
        "diem_tb_tieu_chi": {"$first": "$r.diem_tb_tieu_chi"}
    }},
    {"$sort": {"user_rating_mean": -1}}
]
df = pd.DataFrame(list(b3.aggregate(pipeline))).rename(columns={"_id": "restaurant_url"})
df.to_csv(os.path.join(OUTPUT_DIR, "Q9_rating_vs_criteria.csv"), index=False, encoding="utf-8-sig")
print(" Q9_rating_vs_criteria.csv")


# Q10: Top quán “đáng tin” để đề xuất
# score = user_rating_mean * log1p(review_count)
pipeline = [
    {"$match": {"user_rating": {"$ne": None}}},
    {"$group": {
        "_id": "$restaurant_url",
        "restaurant_name": {"$first": "$restaurant_name"},
        "district": {"$first": "$district"},
        "user_rating_mean": {"$avg": "$user_rating"},
        "review_count": {"$sum": 1}
    }}
]

rows = []
for d in b3.aggregate(pipeline):
    mean_rating = d.get("user_rating_mean")
    if mean_rating is None:
        continue
    d["recommend_score"] = round(mean_rating * math.log1p(d["review_count"]), 3)
    rows.append(d)

df = pd.DataFrame(rows).rename(columns={"_id": "restaurant_url"})
df = df.sort_values("recommend_score", ascending=False).head(10)

df.to_csv(os.path.join(OUTPUT_DIR, "Q10_recommended_restaurants.csv"), index=False, encoding="utf-8-sig")
print(" Q10_recommended_restaurants.csv")

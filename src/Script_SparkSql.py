from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigDataProject_Nhom14") \
    .getOrCreate()

# Nếu bạn cần đọc lại dữ liệu:
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/doannhom14/mobile_ai_app_usage.csv")

df.createOrReplaceTempView("ai_usage")

# Giả sử đã có: df_clean.createOrReplaceTempView("ai_usage")

# 1
print("\n1. Đếm số user duy nhất mỗi quốc gia")
spark.sql("""
SELECT country, COUNT(DISTINCT user_id) AS user_count
FROM ai_usage
GROUP BY country
ORDER BY user_count DESC
""").show()

# 2
print("\n2. Thời gian sử dụng trung bình của từng ứng dụng theo hệ điều hành")
spark.sql("""
SELECT app_name, device_os, ROUND(AVG(usage_minute), 2) AS avg_usage_minute
FROM ai_usage
GROUP BY app_name, device_os
ORDER BY app_name, device_os
""").show()


# 3
print("\n3. Top 5 ứng dụng AI có tổng thời lượng sử dụng cao nhất")
spark.sql("""
SELECT app_name, SUM(usage_minute) AS total_usage
FROM ai_usage
GROUP BY app_name
ORDER BY total_usage DESC
LIMIT 5
""").show()

# 4
print("\n4. Tỷ lệ người dùng Premium theo quốc gia")
spark.sql("""
SELECT country,
    COUNT(DISTINCT CASE WHEN payment_type = 'Premium' THEN user_id END) AS premium_users,
    COUNT(DISTINCT user_id) AS total_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN payment_type = 'Premium' THEN user_id END) / COUNT(DISTINCT user_id), 2) AS premium_percent
FROM ai_usage
GROUP BY country
ORDER BY premium_percent DESC
""").show()

# 5
print("\n5. Trung bình rating từng loại network")
spark.sql("""
SELECT network_type, AVG(user_rating) AS avg_rating
FROM ai_usage
GROUP BY network_type
ORDER BY avg_rating DESC
""").show()

# 6
print("\n6. Tỷ lệ sử dụng các app AI theo độ tuổi")
spark.sql("""
SELECT 
  CASE 
    WHEN age < 18 THEN 'Teen'
    WHEN age <= 25 THEN '18-25'
    WHEN age <= 35 THEN '26-35'
    WHEN age <= 50 THEN '36-50'
    ELSE '51-60'
  END AS age_group,
  app_name,
  COUNT(DISTINCT user_id) AS user_count
FROM ai_usage
GROUP BY age_group, app_name
ORDER BY age_group, user_count DESC
""").show()

# 7
print("7. Số lượng phiên sử dụng của từng ứng dụng AI theo loại thanh toán")
spark.sql("""
SELECT
    app_name,
    payment_type,
    COUNT(session_id) AS total_sessions
FROM ai_usage
GROUP BY app_name, payment_type
ORDER BY total_sessions DESC
""").show()

# 8
print('8. Phân tích khung giờ sử dụng cao điểm trong ngày')
spark.sql("""
SELECT HOUR(usage_time) AS usage_hour,
       COUNT(*) AS session_count
FROM ai_usage
GROUP BY HOUR(usage_time)
ORDER BY session_count DESC
""").show()


# 9
print("\n9. Ứng dụng có tỷ lệ chuyển đổi từ Free sang Premium cao nhất")
spark.sql("""
SELECT app_name,
  COUNT(DISTINCT CASE WHEN payment_type = 'Premium' THEN user_id END) AS premium_users,
  COUNT(DISTINCT user_id) AS total_users,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN payment_type = 'Premium' THEN user_id END) / COUNT(DISTINCT user_id), 2) AS premium_rate
FROM ai_usage
GROUP BY app_name
ORDER BY premium_rate DESC
""").show()

# 10
print("\n10. Tính tỷ lệ user có rating trung bình >4.0 theo từng quốc gia")
spark.sql("""
SELECT country,
  COUNT(DISTINCT CASE WHEN avg_rating > 4.0 THEN user_id END) AS high_rating_users,
  COUNT(DISTINCT user_id) AS total_users,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN avg_rating > 4.0 THEN user_id END) / COUNT(DISTINCT user_id), 2) AS high_rating_percent
FROM (
  SELECT country, user_id, AVG(user_rating) AS avg_rating
  FROM ai_usage
  GROUP BY country, user_id
)
GROUP BY country
ORDER BY high_rating_percent DESC
""").show()

# 11
print("\n11. Phân tích mối quan hệ giữa thời lượng sử dụng và mức đánh giá theo session ngắn/vừa/dài")
spark.sql("""
SELECT
  CASE
    WHEN usage_minute <= 10 THEN 'short'
    WHEN usage_minute <= 60 THEN 'medium'
    ELSE 'long'
  END AS session_length,
  COUNT(*) AS total_sessions,
  ROUND(AVG(user_rating), 3) AS avg_rating,
  ROUND(
    100.0 * SUM(CASE WHEN user_rating >= 4.0 THEN 1 ELSE 0 END) / COUNT(*), 2
  ) AS high_rating_percent
FROM ai_usage
GROUP BY
  CASE
    WHEN usage_minute <= 10 THEN 'short'
    WHEN usage_minute <= 60 THEN 'medium'
    ELSE 'long'
  END
""").show()

# 12
print('12. Nhóm tính năng nào có rating trung bình cao nhất?')
spark.sql("""
SELECT feature,
       ROUND(AVG(user_rating), 3) AS avg_rating,
       COUNT(*) AS session_count
FROM (
    SELECT explode(split(features_used, ' ')) AS feature, user_rating
    FROM ai_usage
) tmp
GROUP BY feature
HAVING COUNT(*) > 30
ORDER BY avg_rating DESC
LIMIT 10
""").show()

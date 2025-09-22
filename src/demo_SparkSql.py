from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigDataProject_Nhom14") \
    .getOrCreate()

# Nếu bạn cần đọc lại dữ liệu:
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/doannhom14/mobile_ai_app_usage.csv")

df.createOrReplaceTempView("ai_usage")

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
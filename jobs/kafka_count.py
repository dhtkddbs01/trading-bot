"""클러스터 → Kafka(ticks.raw) 연결 시험 — 메시지 개수 세기 (클러스터 4단계).

데스크탑 Kafka 는 비어 있어서 0건이 정상. 실제 데이터는 서버 Kafka 에 있다.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkKafkaStreaming").getOrCreate()

# Kafka 읽기 — read(배치)라 지금 쌓인 것까지만 읽고 끝남, earliest = 처음 메시지부터
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "ticks.raw") \
    .option("startingOffsets", "earliest") \
    .load()

# 전체 개수 → 파티션별 개수 → 메시지 내용 (key · value 는 bytes 라 문자열로 바꿔서 출력)
total_count = kafka_df.count()
print(f"Total count: {total_count}")
partition_count_df = kafka_df.groupBy("partition").count()
partition_count_df.show()
value_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
value_df.show(truncate=False)
spark.stop()
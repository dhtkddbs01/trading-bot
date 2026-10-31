"""Kafka(ticks.raw) → bronze Parquet — 착수 5단계.

받은 원본을 파싱하지 않고 그대로 날짜별로 저장한다. 어디까지 읽었는지(offset)는 체크포인트에 남아서,
다시 돌리면 그 다음 메시지부터 읽는다. availableNow — 지금까지 쌓인 것만 읽고 끝난다.

실행 방법은 Spark Standalone 클러스터를 구축하면서 정한다.
"""
from pyspark.sql import SparkSession, functions as F

KAFKA_BOOTSTRAP = "kafka:19092"                   # 컨테이너끼리 쓰는 주소
TOPIC = "ticks.raw"
OUTPUT = "data/bronze/ticks"                      # 결과 Parquet — date=YYYY-MM-DD 폴더로 나뉜다
CHECKPOINT = "data/checkpoints/bronze_ticks"      # 어디까지 읽었는지 — 지우면 처음부터 다시 읽는다


def main() -> None:
    spark = SparkSession.builder.appName("bronze-ticks").getOrCreate()
    # TODO 0: 시간대(Asia/Seoul)와 Kafka 연결 모듈(spark-sql-kafka)을 어디서 넣을지 — 클러스터 구축 때 정한다

    # TODO 1: Kafka 를 스트림으로 읽기 — spark.readStream.format("kafka")
    #         옵션: kafka.bootstrap.servers, subscribe, startingOffsets
    #         (startingOffsets 는 체크포인트가 없는 첫 실행에만 쓰인다)

    # TODO 2: 저장할 컬럼 고르기 — 원본은 파싱하지 않는다
    #         key → code (bytes 를 문자열로), value → raw (문자열로), partition, offset,
    #         timestamp → kafka_ts, kafka_ts 에서 날짜만 뽑아 date

    # TODO 3: Parquet 으로 쓰기 — .writeStream.format("parquet")
    #         path, checkpointLocation, partitionBy("date"), trigger(availableNow=True)

    # TODO 4: 다 읽을 때까지 기다리기 — start() 가 돌려준 쿼리의 awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()

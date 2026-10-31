# Spark 이미지에 서버 사용자와 같은 uid 1000 계정을 추가한다.
# 기본 사용자(spark, uid 185)는 서버의 data/ 에 쓸 수 없고, 계정 없이 uid 1000 으로만 돌리면
# Spark(Hadoop)가 실행 사용자 이름을 못 찾아 실패한다 ("Invalid UID, could not determine effective user").
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
USER root
RUN groupadd --gid 1000 app && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /opt/spark/work-dir app
USER app

FROM bitnamilegacy/spark:3.5.3

USER root
RUN pip install --no-cache-dir pymongo

WORKDIR /opt/spark-jobs

COPY analysis.py utils.py ./

ENV SPARK_MASTER=spark://spark-master:7077
ENV PARQUET_PATH=/data/parquet
ENV MONGO_URI=mongodb://admin:gdelt2026@mongodb:27017/gdelt_analytics?authSource=admin

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0", \
     "/opt/spark-jobs/analysis.py"]

import os
import json
import io
import time
import logging
from datetime import datetime, timedelta

import pytz
import holidays
import pandas as pd
from azure.storage.blob import BlobServiceClient
from confluent_kafka import Consumer, TopicPartition
from apscheduler.schedulers.blocking import BlockingScheduler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# CONFIG
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("job-b")

ACCOUNT_NAME = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
ACCOUNT_KEY  = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
CONTAINER    = os.environ.get("AZURE_CONTAINER_NAME", "nyc-traffic-lakehouse")
CONN_STR     = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

KAFKA_TOPIC = "nyc-traffic-snapshots"
EASTERN = pytz.timezone("America/New_York")

BASE_PATH  = f"wasbs://{CONTAINER}@{ACCOUNT_NAME}.blob.core.windows.net"
MODEL_PATH = f"{BASE_PATH}/artifacts/model/random_forest_model"

KAFKA_LOOKBACK_MIN = 90

# Mapping Python weekday() -> Spark dayofweek() convention (1=CN, 7=T7)
WEEKDAY_TO_SPARK_DOW = {0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}

# Lag mốc cần thiết: phút mục tiêu -> (khoảng tìm ưu tiên 1: lo, hi)
LAG_SPECS = {
    15: (10, 20),
    30: (25, 35),
    45: (40, 50),
    60: (55, 65),
}

blob_service = BlobServiceClient.from_connection_string(CONN_STR)

# State giữ trong memory suốt vòng đời container
free_flow_speed_lookup = {}   # link_id -> {free_flow_speed, borough, link_name}
us_holidays = holidays.US(subdiv="NY", years=[2024, 2025, 2026])
holiday_dates = set(str(d) for d in us_holidays.keys())

spark = None
model = None
kafka_consumer = None


# INIT: SparkSession
def init_spark():
    global spark
    spark = (SparkSession.builder
        .appName("Job_B_Predictor")
        .master("spark://spark-master:7077")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4")
        .config(f"spark.hadoop.fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net", ACCOUNT_KEY)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark version: {spark.version}, master: {spark.sparkContext.master}")


# INIT: load PipelineModel + free_flow_speed_lookup từ Azure
def load_model_and_lookup():
    global model, free_flow_speed_lookup

    model = PipelineModel.load(MODEL_PATH)
    logger.info(f"Loaded PipelineModel from {MODEL_PATH}")

    blob = blob_service.get_blob_client(container=CONTAINER, blob="artifacts/free_flow_speed_lookup.parquet")
    data = blob.download_blob().readall()
    df = pd.read_parquet(io.BytesIO(data))

    for _, row in df.iterrows():
        free_flow_speed_lookup[str(row["link_id"])] = {
            "free_flow_speed": float(row["free_flow_speed"]),
            "borough": row["borough"],
            "link_name": row["link_name"]
        }
    logger.info(f"Loaded free_flow_speed_lookup: {len(free_flow_speed_lookup)} links")


# INIT: Kafka Consumer (giữ trong memory suốt vòng đời container)
def init_kafka_consumer():
    global kafka_consumer
    kafka_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "job-b-predictor",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False
    })
    logger.info("Kafka Consumer initialized")


# Query Kafka: lấy toàn bộ message 90 phút gần nhất theo link_id
def query_kafka_snapshots(_retry=0):
    global kafka_consumer
    # Assign thủ công các partition, seek về now-90min, đọc hết rồi dừng poll
    # Trả về dict: link_id -> list snapshot dict {data_as_of, current_congestion, speed_ratio, link_name}
    # Trả về None nếu Kafka không phản hồi
    try:
        md = kafka_consumer.list_topics(KAFKA_TOPIC, timeout=10)
        if KAFKA_TOPIC not in md.topics:
            logger.error(f"Topic {KAFKA_TOPIC} không tồn tại")
            return None
        partitions = list(md.topics[KAFKA_TOPIC].partitions.keys())

        now_et = datetime.now(EASTERN)
        cutoff = now_et - timedelta(minutes=KAFKA_LOOKBACK_MIN)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        tps = [TopicPartition(KAFKA_TOPIC, p, cutoff_ms) for p in partitions]
        offsets = kafka_consumer.offsets_for_times(tps, timeout=10)

        assign_tps = []
        for tp in offsets:
            if tp.offset is not None and tp.offset >= 0:
                offset = tp.offset
            else:
                _, high = kafka_consumer.get_watermark_offsets(
                    TopicPartition(KAFKA_TOPIC, tp.partition), timeout=10
                )
                offset = high
            assign_tps.append(TopicPartition(KAFKA_TOPIC, tp.partition, offset))

        kafka_consumer.assign(assign_tps)

        snapshots_by_link = {}
        empty_polls = 0
        while empty_polls < 3:
            msg = kafka_consumer.poll(timeout=2.0)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue

            link_id = msg.key().decode("utf-8") if msg.key() else None
            if link_id is None:
                continue
            try:
                value = json.loads(msg.value().decode("utf-8"))
                data_as_of = datetime.fromisoformat(value["data_as_of"])
                if data_as_of.tzinfo is None:
                    data_as_of = EASTERN.localize(data_as_of)
                snapshot = {
                    "data_as_of": data_as_of,
                    "current_congestion": value["current_congestion"],
                    "speed_ratio": value["speed_ratio"],
                    "link_name": value.get("link_name")
                }
                snapshots_by_link.setdefault(link_id, []).append(snapshot)
            except Exception:
                continue

        for link_id in snapshots_by_link:
            snapshots_by_link[link_id].sort(key=lambda s: s["data_as_of"])

        return snapshots_by_link

    except Exception as e:
        logger.warning(f"Kafka lỗi: {e}, đang retry...")
        try:
            kafka_consumer.close()
        except Exception:
            pass
        init_kafka_consumer()
        time.sleep(3)
        if _retry >= 1:
            logger.error("Kafka không phản hồi sau retry, skip lần chạy này")
            return None
        return query_kafka_snapshots(_retry=_retry + 1)


# Tìm lag feature: Ưu tiên 1 (±5 phút quanh mốc) -> Ưu tiên 2 (LOCF)
def find_lag_snapshot(snapshots: list, ref_time: datetime, target_minutes: int, lo: int, hi: int):
    # snapshots đã sort theo data_as_of tăng dần. Trả về snapshot dict hoặc None nếu MISSING
    target_time = ref_time - timedelta(minutes=target_minutes)

    window_start = ref_time - timedelta(minutes=hi)
    window_end = ref_time - timedelta(minutes=lo)

    candidates = [s for s in snapshots if window_start <= s["data_as_of"] <= window_end]
    if candidates:
        best = min(candidates, key=lambda s: abs((s["data_as_of"] - target_time).total_seconds()))
        return best

    # LOCF: snapshot hợp lệ gần nhất TRƯỚC mốc target_time
    before = [s for s in snapshots if s["data_as_of"] <= target_time]
    if not before:
        return None  # MISSING

    locf = max(before, key=lambda s: s["data_as_of"])
    gap_minutes = (target_time - locf["data_as_of"]).total_seconds() / 60
    if gap_minutes > 15:
        return None  # MISSING

    return locf


# Tính time features từ timestamp (mapping Spark convention)
def compute_time_features(dt_et: datetime):
    hour = dt_et.hour
    day_of_week = WEEKDAY_TO_SPARK_DOW[dt_et.weekday()]
    month = dt_et.month
    is_weekend = 1 if day_of_week in (1, 7) else 0
    date_str = dt_et.strftime("%Y-%m-%d")
    is_holiday = 1 if date_str in holiday_dates else 0
    return hour, day_of_week, month, is_weekend, is_holiday


# Build feature row cho 1 link. Trả về None nếu thiếu lag (MISSING) hoặc link không có lookup.
def build_feature_row(link_id: str, snapshots: list):
    if link_id not in free_flow_speed_lookup:
        return None

    latest = max(snapshots, key=lambda s: s["data_as_of"])
    current_congestion = latest["current_congestion"]
    speed_ratio = latest["speed_ratio"]
    data_as_of = latest["data_as_of"]

    hour, day_of_week, month, is_weekend, is_holiday = compute_time_features(data_as_of)

    lag_values = {}
    for minutes, (lo, hi) in LAG_SPECS.items():
        snap = find_lag_snapshot(snapshots, data_as_of, minutes, lo, hi)
        if snap is None:
            return None  # MISSING -> bỏ qua link này
        lag_values[minutes] = snap

    past_congestion_15min = lag_values[15]["current_congestion"]
    past_speed_ratio_15min = lag_values[15]["speed_ratio"]

    congestion_trend = current_congestion - past_congestion_15min
    speed_ratio_trend = speed_ratio - past_speed_ratio_15min

    borough = free_flow_speed_lookup[link_id]["borough"]
    link_name = latest.get("link_name") or free_flow_speed_lookup[link_id].get("link_name")

    return {
        "link_id": link_id,
        "borough": borough,
        "link_name": link_name,
        "speed_ratio": speed_ratio,
        "current_congestion": current_congestion,
        "hour": hour,
        "day_of_week": day_of_week,
        "month": month,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "past_congestion_15min": lag_values[15]["current_congestion"],
        "past_speed_ratio_15min": lag_values[15]["speed_ratio"],
        "past_congestion_30min": lag_values[30]["current_congestion"],
        "past_speed_ratio_30min": lag_values[30]["speed_ratio"],
        "past_congestion_45min": lag_values[45]["current_congestion"],
        "past_speed_ratio_45min": lag_values[45]["speed_ratio"],
        "past_congestion_60min": lag_values[60]["current_congestion"],
        "past_speed_ratio_60min": lag_values[60]["speed_ratio"],
        "congestion_trend": congestion_trend,
        "speed_ratio_trend": speed_ratio_trend,
        "data_as_of": data_as_of,
    }


# Ghi kết quả ra Azure gold/predictions/, retry 1 lần nếu fail
def write_predictions(records: list):
    if not records:
        logger.info("Không có prediction nào để ghi")
        return

    now_et = datetime.now(EASTERN)
    date_str = now_et.strftime("%Y-%m-%d")
    hour_str = now_et.strftime("%H")
    blob_path = f"gold/predictions/date={date_str}/hour={hour_str}/part-{int(time.time())}.parquet"

    df = pd.DataFrame(records)

    for attempt in range(2):
        try:
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            blob = blob_service.get_blob_client(container=CONTAINER, blob=blob_path)
            blob.upload_blob(buf, overwrite=True)
            logger.info(f"Ghi predictions xong: {blob_path} ({len(records)} rows)")
            return
        except Exception as e:
            logger.warning(f"Ghi predictions fail (attempt {attempt+1}/2): {e}")
            time.sleep(1)

    logger.error(f"Ghi predictions fail sau retry, bỏ qua batch: {blob_path}")


# Main job: chạy mỗi 5 phút
def run_predictor():
    snapshots_by_link = query_kafka_snapshots()
    if snapshots_by_link is None:
        return  # Kafka không phản hồi, đã log error

    feature_rows = []
    for link_id, snapshots in snapshots_by_link.items():
        row = build_feature_row(link_id, snapshots)
        if row is not None:
            feature_rows.append(row)

    if not feature_rows:
        logger.warning("Insufficient history, skipping all links — warming up")
        return

    pdf = pd.DataFrame(feature_rows)
    pdf["link_id"] = pdf["link_id"].astype(str)
    pdf["borough"] = pdf["borough"].astype(str)
    sdf = spark.createDataFrame(pdf)

    predictions = model.transform(sdf)
    predictions = predictions.withColumn("confidence", F.array_max(vector_to_array("probability")))

    result_pdf = predictions.select(
        "link_id", "borough", "link_name", "data_as_of",
        F.col("prediction").alias("predicted_congestion"),
        "confidence"
    ).toPandas()

    if len(result_pdf) < len(feature_rows):
        logger.info(f"Spark tự skip {len(feature_rows) - len(result_pdf)} link không có trong StringIndexer")

    output_records = []
    for _, row in result_pdf.iterrows():
        timestamp = row["data_as_of"]
        target_time = timestamp + timedelta(minutes=15)
        output_records.append({
            "link_id": row["link_id"],
            "borough": row["borough"],
            "link_name": row["link_name"],
            "timestamp": timestamp,
            "target_time": target_time,
            "predicted_congestion": int(row["predicted_congestion"]),
            "confidence": float(row["confidence"])
        })

    logger.info(f"Predict xong: {len(output_records)} links")
    write_predictions(output_records)


# Entry point
if __name__ == "__main__":
    logger.info("Job B starting...")
    init_spark()
    load_model_and_lookup()
    init_kafka_consumer()

    scheduler = BlockingScheduler(timezone="America/New_York")
    scheduler.add_job(
        run_predictor, "cron", minute="*/5", second=0,
        max_instances=1,
        misfire_grace_time=60
    )

    logger.info("Scheduler bắt đầu (mỗi 5 phút)...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Job B dừng.")
        kafka_consumer.close()
        spark.stop()
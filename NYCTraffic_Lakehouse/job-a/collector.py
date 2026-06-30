import os
import json
import io
import time
import logging
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from confluent_kafka import Producer, Consumer, TopicPartition
from apscheduler.schedulers.blocking import BlockingScheduler

# CONFIG
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("job-a")

AZURE_CONN_STR  = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER_NAME", "nyc-traffic-lakehouse")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

KAFKA_TOPIC = "nyc-traffic-snapshots"
REALTIME_FEED_URL = "https://linkdata.nyctmc.org/data/LinkSpeedQuery.txt"
EASTERN = pytz.timezone("America/New_York")

WARMUP_MIN_LINKS = 100   # < 100 distinct link trong Kafka 90 phút gần nhất -> warm-up
LINK_DEAD_MINUTES = 120  # DataAsOf cũ hơn 120 phút -> coi là link chết

blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# State giữ trong memory suốt vòng đời container
free_flow_speed_lookup = {}   # link_id -> {free_flow_speed, borough, link_name}
active_link_ids = set()
last_pushed_data_as_of = {}   # link_id -> data_as_of string (lần push gần nhất)


# INIT: load lookup + active_link_ids khi container start
def load_lookup_and_active_links():
    global free_flow_speed_lookup, active_link_ids

    blob = blob_service.get_blob_client(container=AZURE_CONTAINER, blob="artifacts/active_link_ids.json")
    active_link_ids = set(json.loads(blob.download_blob().readall()))
    logger.info(f"Loaded active_link_ids: {len(active_link_ids)} links")

    blob = blob_service.get_blob_client(container=AZURE_CONTAINER, blob="artifacts/free_flow_speed_lookup.parquet")
    data = blob.download_blob().readall()
    df = pd.read_parquet(io.BytesIO(data))

    for _, row in df.iterrows():
        free_flow_speed_lookup[str(row["link_id"])] = {
            "free_flow_speed": float(row["free_flow_speed"]),
            "borough": row["borough"],
            "link_name": row["link_name"]
        }
    logger.info(f"Loaded free_flow_speed_lookup: {len(free_flow_speed_lookup)} links")


# Helper: parse TSV feed real-time
def fetch_realtime_feed():
    # Gọi linkdata.nyctmc.org. Timeout 15s, KHÔNG retry (intentional design)
    resp = requests.get(REALTIME_FEED_URL, timeout=15)
    resp.raise_for_status()
    lines = resp.text.strip().split("\n")
    header = lines[0].split("\t")
    rows = [dict(zip(header, line.split("\t"))) for line in lines[1:]]
    return rows


def parse_row(row):
    # Parse 1 row TSV -> dict sạch. Key có dấu nháy kép bao quanh
    link_id    = row['"linkId"'].strip().strip('"')
    speed_str  = row['"Speed"'].strip().strip('"')
    daof_str   = row['"DataAsOf"'].strip().strip('"')
    link_name  = row.get('"linkName"', '').strip().strip('"')

    speed = float(speed_str) if speed_str else None

    dt_naive = datetime.strptime(daof_str, "%m/%d/%Y %H:%M:%S")
    dt_et = EASTERN.localize(dt_naive)

    return {
        "link_id": link_id,
        "speed": speed,
        "data_as_of": dt_et,
        "link_name": link_name
    }


def compute_speed_ratio_congestion(speed: float, link_id: str):
    # Trả về (speed_ratio, current_congestion) hoặc (None, None) nếu link không có trong lookup
    if link_id not in free_flow_speed_lookup:
        return None, None
    ffs = free_flow_speed_lookup[link_id]["free_flow_speed"]
    speed_ratio = speed / ffs
    if speed_ratio >= 0.67:
        current_congestion = 0
    elif speed_ratio >= 0.40:
        current_congestion = 1
    else:
        current_congestion = 2
    return speed_ratio, current_congestion


# Push Kafka + ghi Azure Bronze realtime
def push_to_kafka(link_id: str, data_as_of: datetime, current_congestion: int, speed_ratio: float, link_name: str):
    value = {
        "data_as_of": data_as_of.isoformat(),
        "current_congestion": current_congestion,
        "speed_ratio": speed_ratio,
        "link_name": link_name
    }
    producer.produce(KAFKA_TOPIC, key=link_id, value=json.dumps(value))


def write_bronze_realtime(records: list):
    # Append raw snapshot ra bronze/dot_traffic_speeds/realtime/date=/hour=/ trên Azure.
    # Nếu fail -> retry 1 lần -> vẫn fail -> log error, bỏ qua (không ảnh hưởng Kafka push)
    if not records:
        return

    now_et = datetime.now(EASTERN)
    date_str = now_et.strftime("%Y-%m-%d")
    hour_str = now_et.strftime("%H")
    blob_path = f"bronze/dot_traffic_speeds/realtime/date={date_str}/hour={hour_str}/part-{int(time.time())}.parquet"

    df = pd.DataFrame(records)

    for attempt in range(2):  # 1 lần gốc + 1 lần retry
        try:
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            blob = blob_service.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)
            blob.upload_blob(buf, overwrite=True)
            logger.info(f"Bronze realtime ghi xong: {blob_path} ({len(records)} rows)")
            return
        except Exception as e:
            logger.warning(f"Ghi Bronze realtime fail (attempt {attempt+1}/2): {e}")
            time.sleep(1)

    logger.error(f"Ghi Bronze realtime fail sau retry, bỏ qua: {blob_path}")


# Warm-up logic
def count_distinct_links_in_kafka_90min() -> int:
    # Đếm số distinct link_id có message trong Kafka 90 phút gần nhất
    # Dừng poll khi 3 lần liên tiếp không nhận được message mới (an toàn hơn so sánh offset)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"job-a-warmup-check-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False
    })

    try:
        md = consumer.list_topics(KAFKA_TOPIC, timeout=10)
        if KAFKA_TOPIC not in md.topics:
            return 0
        partitions = list(md.topics[KAFKA_TOPIC].partitions.keys())

        cutoff_ms = int((datetime.now(EASTERN) - timedelta(minutes=90)).timestamp() * 1000)
        tps = [TopicPartition(KAFKA_TOPIC, p, cutoff_ms) for p in partitions]
        offsets = consumer.offsets_for_times(tps, timeout=10)

        assign_tps = []
        for tp in offsets:
            offset = tp.offset if tp.offset is not None and tp.offset >= 0 else 0
            assign_tps.append(TopicPartition(KAFKA_TOPIC, tp.partition, offset))
        consumer.assign(assign_tps)

        distinct_links = set()
        empty_polls = 0
        while empty_polls < 3:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            if msg.key():
                distinct_links.add(msg.key().decode("utf-8"))

        return len(distinct_links)
    except Exception as e:
        logger.warning(f"Không đếm được distinct link trong Kafka: {e}")
        return 0
    finally:
        consumer.close()


def warmup_from_bronze_realtime():
    # Đọc 2 partition Bronze realtime liên tiếp gần nhất, tính speed_ratio/current_congestion, push Kafka
    now_et = datetime.now(EASTERN)
    prev_hour_dt = now_et - timedelta(hours=1)

    partitions_to_read = [
        f"date={now_et.strftime('%Y-%m-%d')}/hour={now_et.strftime('%H')}",
        f"date={prev_hour_dt.strftime('%Y-%m-%d')}/hour={prev_hour_dt.strftime('%H')}",
    ]

    cutoff = now_et - timedelta(minutes=90)
    container_client = blob_service.get_container_client(AZURE_CONTAINER)

    pushed = 0
    for partition in partitions_to_read:
        prefix = f"bronze/dot_traffic_speeds/realtime/{partition}/"
        try:
            blobs = list(container_client.list_blobs(name_starts_with=prefix))
        except Exception as e:
            logger.warning(f"Warm-up: không đọc được partition {prefix}: {e}")
            continue

        for b in blobs:
            if not b.name.endswith(".parquet"):
                continue
            try:
                data = blob_service.get_blob_client(container=AZURE_CONTAINER, blob=b.name).download_blob().readall()
                df = pd.read_parquet(io.BytesIO(data))
                df["data_as_of"] = pd.to_datetime(df["data_as_of"])
                df = df[df["data_as_of"] >= cutoff]

                for _, row in df.iterrows():
                    link_id = str(row["link_id"])
                    speed = row["speed"]
                    speed_ratio, current_congestion = compute_speed_ratio_congestion(speed, link_id)
                    if speed_ratio is None:
                        continue
                    push_to_kafka(
                        link_id=link_id,
                        data_as_of=row["data_as_of"],
                        current_congestion=current_congestion,
                        speed_ratio=speed_ratio,
                        link_name=row.get("link_name", "")
                    )
                    pushed += 1
            except Exception as e:
                logger.warning(f"Warm-up: lỗi đọc file {b.name}: {e}")

    if pushed > 0:
        producer.flush()
    logger.info(f"Warm-up xong: pushed {pushed} snapshots lên Kafka")


consecutive_healthy_runs = 0
HEALTHY_CHECK_INTERVAL = 30  # chỉ check lại warm-up mỗi 30 phút khi đã ổn định
def maybe_warmup():
    global consecutive_healthy_runs

    # Nếu đã ổn định lâu rồi, chỉ check định kỳ thay vì mỗi phút
    if consecutive_healthy_runs > 0 and consecutive_healthy_runs % HEALTHY_CHECK_INTERVAL != 0:
        consecutive_healthy_runs += 1
        return

    distinct_count = count_distinct_links_in_kafka_90min()
    if distinct_count >= WARMUP_MIN_LINKS:
        consecutive_healthy_runs += 1
        return  # đủ data, bỏ qua warm-up

    consecutive_healthy_runs = 0  # reset vì đang thiếu data
    logger.warning(f"Kafka chỉ có {distinct_count} distinct links (<{WARMUP_MIN_LINKS}) - kiểm tra warm-up")

    container_client = blob_service.get_container_client(AZURE_CONTAINER)
    has_bronze_realtime = any(container_client.list_blobs(
        name_starts_with="bronze/dot_traffic_speeds/realtime/", results_per_page=1
    ))

    if not has_bronze_realtime:
        logger.warning("Bronze realtime trống (lần đầu deploy) - bỏ qua warm-up, hệ thống sẽ tự tích lũy data")
        return

    warmup_from_bronze_realtime()


# Main job: chạy mỗi 1 phút
def run_collector():
    maybe_warmup()

    try:
        rows = fetch_realtime_feed()
    except Exception as e:
        logger.error(f"Gọi linkdata.nyctmc.org thất bại, skip lần chạy này: {e}")
        return

    now_et = datetime.now(EASTERN)
    dead_cutoff = now_et - timedelta(minutes=LINK_DEAD_MINUTES)

    bronze_records = []
    pushed_count = 0

    for raw_row in rows:
        try:
            parsed = parse_row(raw_row)
        except Exception:
            continue

        link_id = parsed["link_id"]
        speed = parsed["speed"]
        data_as_of = parsed["data_as_of"]
        link_name = parsed["link_name"]

        # CASE 1 - link chết
        if data_as_of < dead_cutoff:
            continue

        # CASE 2 - sensor lỗi tạm thời
        if speed is None or speed <= 0 or speed >= 100:
            continue

        # CASE 3 - data hợp lệ
        prev_daof = last_pushed_data_as_of.get(link_id)
        if prev_daof == data_as_of.isoformat():
            continue  # DataAsOf không đổi, skip

        if link_id not in free_flow_speed_lookup:
            logger.warning(f"Link {link_id} không có trong free_flow_speed_lookup, bỏ qua")
            continue

        speed_ratio, current_congestion = compute_speed_ratio_congestion(speed, link_id)

        push_to_kafka(link_id, data_as_of, current_congestion, speed_ratio, link_name)
        last_pushed_data_as_of[link_id] = data_as_of.isoformat()
        pushed_count += 1

        bronze_records.append({
            "link_id": link_id,
            "speed": speed,
            "data_as_of": data_as_of,
            "borough": free_flow_speed_lookup[link_id]["borough"],
            "link_name": link_name
        })

    producer.flush()
    logger.info(f"Run xong: {pushed_count} snapshots pushed lên Kafka")

    write_bronze_realtime(bronze_records)


# Entry point
if __name__ == "__main__":
    logger.info("Job A starting...")
    load_lookup_and_active_links()

    scheduler = BlockingScheduler(timezone="America/New_York")
    scheduler.add_job(
        run_collector, "cron", minute="*", second=0,
        max_instances=1,      # nếu lần chạy trước chưa xong, skip lần mới thay vì chồng lấn
        misfire_grace_time=30  # cho phép trễ tối đa 30s nếu hệ thống bận
    )

    logger.info("Scheduler bắt đầu (mỗi 1 phút)...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Job A dừng.")
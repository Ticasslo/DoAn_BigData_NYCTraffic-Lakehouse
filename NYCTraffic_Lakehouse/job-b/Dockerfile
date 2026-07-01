FROM python:3.13-slim

WORKDIR /app

# pyspark driver cần JVM local (Java 21, khớp với spark-master/spark-slave) để giao tiếp qua py4j.
# Dùng Eclipse Temurin (Adoptium) APT repo để cài đúng Java 21 ổn định, thay vì tải tarball trực tiếp.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc build-essential librdkafka-dev \
        wget gnupg ca-certificates apt-transport-https && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release; echo $VERSION_CODENAME) main" \
        > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends temurin-21-jdk && \
    apt-get purge -y wget gnupg && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY predictor.py .

CMD ["python", "predictor.py"]
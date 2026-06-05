# PySpark AIS Collision Detector
FROM python:3.11-slim

# Install Java 21 (OpenJDK 17 not available on Debian Trixie)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /app/data /app/output

ENV PYTHONUNBUFFERED=1

CMD ["python", "src/main.py"]
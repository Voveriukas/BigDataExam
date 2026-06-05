FROM python:3.11-slim

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

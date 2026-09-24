FROM python:3.12-slim AS bot-api-build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake g++ git gperf libssl-dev make zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --recursive https://github.com/tdlib/telegram-bot-api.git /src
RUN cmake -S /src -B /src/build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /src/build --target install -j2

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TELEGRAM_API_BASE_URL=http://127.0.0.1:8081 \
    DATABASE_PATH=/var/lib/telegram-bot-api/files.sqlite3 \
    BOT_API_DATA_DIR=/var/lib/telegram-bot-api
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates libssl3 libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=bot-api-build /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
COPY templates ./templates
COPY static ./static
COPY run-telegram-bot-api.sh run-render.sh ./
RUN chmod 0755 /app/run-telegram-bot-api.sh /app/run-render.sh \
    && mkdir -p /var/lib/telegram-bot-api
EXPOSE 10000
CMD ["bash", "/app/run-render.sh"]

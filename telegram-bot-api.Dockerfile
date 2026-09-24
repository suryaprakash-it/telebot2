FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake g++ git gperf libssl-dev make zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --recursive https://github.com/tdlib/telegram-bot-api.git /src
RUN cmake -S /src -B /src/build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /src/build --target install -j2

FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates libssl3 libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home botapi \
    && mkdir -p /var/lib/telegram-bot-api \
    && chown -R botapi:botapi /var/lib/telegram-bot-api
COPY --from=build /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api
COPY run-telegram-bot-api.sh /usr/local/bin/run-telegram-bot-api
RUN chmod 0755 /usr/local/bin/run-telegram-bot-api
USER botapi
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/run-telegram-bot-api"]

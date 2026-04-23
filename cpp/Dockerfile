FROM alpine:3.20

RUN apk add --no-cache build-base cmake rabbitmq-c-dev

WORKDIR /app
COPY . .
RUN cmake -S . -B build && cmake --build build -j

ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_PORT=5672
ENV RABBITMQ_USER=guest
ENV RABBITMQ_PASS=guest
ENV EXCHANGE=events.direct
ENV EXCHANGE_TYPE=direct
ENV QUEUE=q.delta

CMD ["./build/consumer_delta"]

FROM golang:1.16-alpine as builder
RUN apk update \
    && apk upgrade \
    && apk --no-cache add git ca-certificates \
    && update-ca-certificates

WORKDIR /build
COPY . .
RUN GOPROXY=https://goproxy.io,direct GOOS=linux go build -o drhcli .

FROM alpine:latest
RUN apk update \
    && apk upgrade

ENV SOURCE_TYPE Amazon_S3

ENV JOB_TABLE_NAME ''
ENV JOB_QUEUE_NAME ''

ENV SRC_BUCKET ''
ENV SRC_PREFIX ''
ENV SRC_REGION ''
ENV SRC_CREDENTIALS ''
ENV SRC_IN_CURRENT_ACCOUNT false

ENV DEST_BUCKET ''
ENV DEST_PREFIX ''
ENV DEST_REGION ''
ENV DEST_CREDENTIALS ''
ENV DEST_IN_CURRENT_ACCOUNT false

ENV MAX_KEYS 1000
ENV CHUNK_SIZE 5
ENV MULTIPART_THRESHOLD 10
ENV MESSAGE_BATCH_SIZE 10
ENV FINDER_DEPTH 0
ENV FINDER_NUMBER 1
ENV WORKER_NUMBER 4

WORKDIR /app
RUN touch config.yaml
COPY --from=builder /build/drhcli .
ENTRYPOINT ["/app/drhcli", "run"]
FROM amazonlinux:latest as builder

RUN yum update -y && yum install -y tar gzip

RUN cd /tmp && \
    curl -LO https://golang.org/dl/go1.16.4.linux-amd64.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf /tmp/go1.16.4.linux-amd64.tar.gz

WORKDIR /build
COPY . .
RUN PATH=$PATH:/usr/local/go/bin GOPROXY=https://goproxy.io,direct GOOS=linux go build -o dthcli .

FROM amazonlinux:latest

RUN yum update -y

ENV SOURCE_TYPE Amazon_S3

ENV JOB_TABLE_NAME ''
ENV JOB_QUEUE_NAME ''

ENV SRC_BUCKET ''
ENV SRC_PREFIX ''
ENV SRC_PREFIX_LIST ''
ENV SRC_REGION ''
ENV SRC_ENDPOINT ''
ENV SRC_CREDENTIALS ''
ENV SRC_IN_CURRENT_ACCOUNT false
ENV SKIP_COMPARE false

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
COPY --from=builder /build/dthcli .
ENTRYPOINT ["/app/dthcli", "run"]
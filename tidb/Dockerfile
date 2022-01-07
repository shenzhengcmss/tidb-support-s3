# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# Builder image
FROM golang:1.16-alpine as builder

RUN apk add --no-cache \
    wget \
    make \
    git \
    gcc \
    binutils-gold \
    musl-dev

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

#RUN mkdir -p /go/src/github.com/pingcap/tidb
#WORKDIR /go/src/github.com/pingcap/tidb

# Cache dependencies
#COPY go.mod .
#COPY go.sum .

#RUN GO111MODULE=on go mod download

# Build real binaries
#COPY . .
#RUN make

# Executable image
FROM alpine:3.11.2

RUN apk add --no-cache
#    curl

RUN mkdir /lib64
RUN ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

COPY ./bin/tidb-server /tidb-server
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/dumb-init

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/usr/local/bin/dumb-init", "/tidb-server"]

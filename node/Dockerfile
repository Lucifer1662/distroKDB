# syntax = docker/dockerfile:1-experimental

# FROM --platform=${BUILDPLATFORM} golang:1.18.0-alpine3.15 AS base

# WORKDIR /app

# RUN apk add build-base

# COPY ./go.* .
# RUN go mod download

# COPY ./ /app

# RUN --mount=type=cache,target=/root/.cache/go-build go build


# FROM golang:1.18.0-alpine3.15

# COPY --from=base / /

FROM golang:1.18.0-alpine3.15

WORKDIR /app

RUN apk add build-base

COPY ./go.* .
RUN go mod download

COPY . .

RUN go build

CMD ./distrokdb
# CMD go run ./main.go
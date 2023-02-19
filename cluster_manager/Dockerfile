FROM golang:1.18.0-alpine3.15

WORKDIR /app

RUN apk add build-base

COPY ./go.* .
RUN go mod download

COPY . .

RUN go build

CMD ./distrokdb
# CMD go run ./main.go
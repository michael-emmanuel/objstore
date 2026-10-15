# One image containing every binary; docker-compose picks the command.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
# wget is used by compose health checks; nothing else is added.
RUN adduser -D -u 10001 objstore && mkdir -p /data && chown objstore /data
COPY --from=build /out/ /usr/local/bin/
USER objstore
VOLUME ["/data"]

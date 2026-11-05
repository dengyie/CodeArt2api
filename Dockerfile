FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /out/codearts2api ./cmd/server && \
    go build -o /out/codearts2api-login ./cmd/login && \
    go build -o /out/codearts2api-credit ./cmd/credit && \
    go build -o /out/codearts2api-apply ./cmd/apply

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S ca2a \
    && adduser -S -G ca2a -h /app ca2a \
    && mkdir -p /app/auths /app/data \
    && chown -R ca2a:ca2a /app
WORKDIR /app
COPY --from=build /out/ /usr/local/bin/
COPY config.example.json ./config.example.json
VOLUME ["/app/auths", "/app/data"]
USER ca2a
EXPOSE 7866
CMD ["codearts2api", "-config", "config.json"]

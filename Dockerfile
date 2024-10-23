FROM alpine:latest
ENTRYPOINT ["/benthos-app"]
COPY benthos-app /

FROM golang:1.14

RUN mkdir -p /build
WORKDIR /build/
COPY . .

# CGO_ENABLED must be disabled to run Go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/monitor github.com/LaCumbancha/yelp-review-ha/cmd/nodes/monitors/common

FROM busybox:latest
COPY --from=0 /build/bin/monitor /monitor
ENTRYPOINT ["/bin/sh"]

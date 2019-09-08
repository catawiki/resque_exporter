FROM golang:1.8 as builder

RUN curl https://glide.sh/get | sh

ENV PKG_NAME=github.com/catawiki/resque_exporter
ENV PKG_PATH=$GOPATH/src/$PKG_NAME
WORKDIR $PKG_PATH

COPY glide.yaml glide.lock $PKG_PATH/
RUN glide install

COPY . $PKG_PATH
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /resque_exporter cmd/resque_exporter/resque_exporter.go

FROM scratch as scratch

COPY --from=builder /resque_exporter /resque_exporter

USER 59000:59000

EXPOSE 5555

ENTRYPOINT ["/resque_exporter"]

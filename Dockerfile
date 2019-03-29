FROM golang:1.8-onbuild

RUN go get -u github.com/go-sql-driver/mysql

CMD ["/usr/local/go/bin/go", "run", "ad.go", "advertiser.go", "main.go"]
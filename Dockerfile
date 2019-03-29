FROM golang:1.8-onbuild

RUN go get -u github.com/go-sql-driver/mysql

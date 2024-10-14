FROM registry.access.redhat.com/ubi9/ubi:9.4-1214.1726694543

RUN dnf install iputils --assumeyes
RUN dnf install golang --assumeyes

WORKDIR /
COPY * .
RUN go build -o app main.go
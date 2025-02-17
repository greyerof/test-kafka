FROM registry.access.redhat.com/ubi9/ubi:9.5-1739751568 as builder

RUN dnf install iputils jq procps gcc --assumeyes --disableplugin=subscription-manager
# Install go version 1.23, which is not available in the pinned ubi9 yet
RUN curl -L https://go.dev/dl/go1.23.2.linux-amd64.tar.gz -o go.tar.gz

RUN checksum=$(sha256sum go.tar.gz) && echo $checksum && \
    test "$checksum" = "542d3c1705f1c6a1c5a80d5dc62e2e45171af291e755d591c5e6531ef63b454e  go.tar.gz"

RUN tar -C /usr/local -xzf go.tar.gz

ENV PATH=${PATH}:/usr/local/go/bin

WORKDIR /
COPY go.* /
COPY main.go .

RUN go build -o kafka-consumer main.go

# New image, without go and gcc.
FROM registry.access.redhat.com/ubi9/ubi:9.5-1739751568
RUN dnf install iputils jq procps --assumeyes --disableplugin=subscription-manager

COPY --from=builder /kafka-consumer .
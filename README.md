# test-kafka
Simple kafka test consumer in go. Since this is a test app, every time the consumer is run, it creates a fake group and consumer id using the unix epoch ts in milliseconds.
For now it only works with SSL kafka servers, so the related cli flags are mandatory.

There's available modes to read kafka messages:
1. By default, the kafka messages are read, polling evey second, and messages are marshalled to json objects.
2. If --ce-mode flag is provided, the kafka messages are decoded as [Cloud Events](https://github.com/cloudevents) using their [sdk-go lib](https://github.com/cloudevents/sdk-go).

Check the prorgam's help (--help) for the available cli flags.

## Build kafka-consumer
```
go build -o kafka-consumer .
```

## CLI flags
```
$ ./kafka-consumer --help
Usage of ./kafka-consumer:
  -bootstrap-server string
        boot-strap server name/ip
  -ca-crt-file string
        ca.crt file location for SSL authentication
  -ce-mode
        CloudEvents mode. If set, kafka messages will be decoded as CloudEvents.
  -ce-type-filters string
        comma separated list of cloud events' type filter regexes. Only events with matching types will be shown.
  -client-crt-file string
        client.crt file location for SSL authentication
  -port int
        bootstrap server port, defaulted to 9093 if not provided (default 9093)
  -ssl-key-file string
        client.key file location for SSL authentication
  -topic string
        kafka topic to consume
```

The docker container image (see [Dockerfile](Dockerfile)) with the kafka-consumer app can be downloaded from greyerof's quay repo [here](https://quay.io/greyerof/kafka-consumer:v0.0.1) with:
```
podman pull quay.io/greyerof/kafka-consumer:v0.0.1
```

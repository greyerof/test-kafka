package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServersFlag *string
	topicFlag            *string
	serverPortFlag       *int

	sslKeyLocationFlag         *string
	caCertLocationFileFlag     *string
	clientCertLocationFileFlag *string
)

func init() {
	bootstrapServersFlag = flag.String("bootstrap-servers", "", "comma separated list of boot-strap servers")
	serverPortFlag = flag.Int("port", 9093, "bootstrap server port")
	topicFlag = flag.String("topic", "", "kafka topic to consume")

	sslKeyLocationFlag = flag.String("ssl-key-file", "", "client.key file location for SSL authentication")
	caCertLocationFileFlag = flag.String("ca-crt-file", "", "ca.crt file location for SSL authentication")
	clientCertLocationFileFlag = flag.String("client-crt-file", "", "client.crt file location for SSL authentication")

	flag.Parse()
}

func getCLIFlagsErrors() []string {
	var errors []string

	if *bootstrapServersFlag == "" {
		errors = append(errors, "flag --bootstrap-servers was not set")
	}

	if *sslKeyLocationFlag == "" {
		errors = append(errors, "flag --ssl-key-file was not set")
	}

	if *caCertLocationFileFlag == "" {
		errors = append(errors, "flag --ca-crt-file was not set")
	}

	if *clientCertLocationFileFlag == "" {
		errors = append(errors, "flag --client-crt-file was not set")
	}

	return errors
}

func main() {
	if errMsgs := getCLIFlagsErrors(); len(errMsgs) > 0 {
		fmt.Println("ERROR:")
		for _, msg := range errMsgs {
			fmt.Printf("  - %s\n", msg)
		}
		os.Exit(0)
	}

	server := fmt.Sprintf("%s:%d", *bootstrapServersFlag, *serverPortFlag)

	config := &kafka.ConfigMap{
		"bootstrap.servers":                   server,
		"group.id":                            "foo",
		"security.protocol":                   "SSL",
		"ssl.key.location":                    *sslKeyLocationFlag,
		"ssl.certificate.location":            *clientCertLocationFileFlag,
		"ssl.ca.location":                     *caCertLocationFileFlag,
		"auto.offset.reset":                   "smallest",
		"enable.ssl.certificate.verification": false,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("ERROR: failed to create consumer: %v\n", err)
		os.Exit(0)
	}

	err = consumer.SubscribeTopics([]string{*topicFlag}, nil)
	if err != nil {
		fmt.Printf("ERROR: failed to subscribe to kafka topic %q: %v\n", *topicFlag, err)
		os.Exit(0)
	}

	defer consumer.Close()

	for {
		fmt.Printf("Polling kafka messages from topic %q, server: %q\n", *topicFlag, server)
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			msg := ev.(*kafka.Message)
			fmt.Printf("Received event msg: %v\n", *msg)
			fmt.Printf("  %% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
		case kafka.Error:
			fmt.Printf("ERROR: failed reading kafka events: %v\n", e)
			os.Exit(0)
		default:
		}
	}
}

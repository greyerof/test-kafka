package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	kafka_confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	ceclient "github.com/cloudevents/sdk-go/v2/client"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	bootstrapServerFlag    *string
	topicFlag              *string
	serverPortFlag         *int
	ceEventTypeFiltersFlag *string

	sslKeyLocationFlag         *string
	caCertLocationFileFlag     *string
	clientCertLocationFileFlag *string

	// compiled regexps from the comma separated list value of --ce-type-filters (if provided)
	evTypeRegexpFilters []*regexp.Regexp

	// read kafka messages as cloud events
	ceModeFlag *bool

	//processCloudEventTypeFilters is used to avoid computing the length of the slice on each msg.
	processCloudEventTypeFilters bool
)

func init() {
	// Kafka server/msg flags
	bootstrapServerFlag = flag.String("bootstrap-server", "", "boot-strap server name/ip")
	serverPortFlag = flag.Int("port", 9093, "bootstrap server port, defaulted to 9093 if not provided")
	topicFlag = flag.String("topic", "", "kafka topic to consume")

	// SSL files
	sslKeyLocationFlag = flag.String("ssl-key-file", "", "client.key file location for SSL authentication")
	caCertLocationFileFlag = flag.String("ca-crt-file", "", "ca.crt file location for SSL authentication")
	clientCertLocationFileFlag = flag.String("client-crt-file", "", "client.crt file location for SSL authentication")

	ceModeFlag = flag.Bool("ce-mode", false, "CloudEvents mode. If set, kafka messages will be decoded as CloudEvents.")

	// CloudEvents related flags
	ceEventTypeFiltersFlag = flag.String("ce-type-filters", "", "comma separated list of cloud events' type filter regexes. Only events with matching types will be shown.")

	flag.Parse()
}

func compileCloudEventTypeRegexFilters(ceTypeFilters string) error {
	ceTypes := strings.Split(ceTypeFilters, ",")

	for _, ceType := range ceTypes {
		if ceType == "" {
			continue
		}

		r, err := regexp.Compile(ceType)
		if err != nil {
			return fmt.Errorf("failed to compile regex for event type filter %q", ceType)
		}

		// Store regexp filter fo cloud event types in global var.
		evTypeRegexpFilters = append(evTypeRegexpFilters, r)
	}

	return nil

}

func getCLIFlagsErrors() []string {
	var errors []string

	if *bootstrapServerFlag == "" {
		errors = append(errors, "flag --bootstrap-server was not set")
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

	if *topicFlag == "" {
		errors = append(errors, "flag --topic was not set")
	}

	if *ceEventTypeFiltersFlag != "" {
		if err := compileCloudEventTypeRegexFilters(*ceEventTypeFiltersFlag); err != nil {
			errors = append(errors, fmt.Sprintf("failed to parse flag --kafka-key: %v", err))
		}

		processCloudEventTypeFilters = len(evTypeRegexpFilters) > 0
	}

	return errors
}

func shouldPrintKafkaMsg(kafkaTopic string) bool {
	for _, regexFilter := range evTypeRegexpFilters {
		if regexFilter.MatchString(kafkaTopic) {
			return true
		}
	}

	return false
}

func readRawKafkaMessages(config *kafka.ConfigMap) {
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("ERROR: failed to create kafka consumer: %v", err)
	}

	err = consumer.SubscribeTopics([]string{*topicFlag}, nil)
	if err != nil {
		log.Fatalf("ERROR: failed to subscribe to kafka topic %q: %v\n", *topicFlag, err)
		os.Exit(0)
	}

	for {
		msg, err := consumer.ReadMessage(1 * time.Second)
		if err != nil {
			if err.(kafka.Error).IsTimeout() {
				continue
			} else {
				log.Fatalf("ERROR: failed to poll kafka queue: %v", err)
			}
		}

		msgBytes, err := json.MarshalIndent(msg, "", "  ")
		if err != nil {
			log.Fatalf("ERROR: failed to marshal kafka msg: %v", err)
		}

		fmt.Println(string(msgBytes))
	}
}

func readCloudEvents(config *kafka.ConfigMap) {
	kafkaProtocol, err := kafka_confluent.New(kafka_confluent.WithConfigMap(config),
		kafka_confluent.WithReceiverTopics([]string{*topicFlag}),
	)
	if err != nil {
		log.Fatalf("ERROR: failed to create kafka protocol client: %v\n", err)
	}

	ceClient, err := cloudevents.NewClient(kafkaProtocol, ceclient.WithBlockingCallback(), ceclient.WithPollGoroutines(1))
	if err != nil {
		log.Fatalf("ERROR: failed to create cloud events client: %v\n", err)
	}

	ceClient.StartReceiver(context.Background(), func(event cloudevents.Event) {
		if processCloudEventTypeFilters && !shouldPrintKafkaMsg(event.Type()) {
			return
		}

		// Print event
		s, err := json.MarshalIndent(&event, "", "  ")
		if err != nil {
			fmt.Printf("ERROR: failed to marshal cloud event: %v\n", err)
			return
		}

		fmt.Printf("%s\n", string(s))
	})
}

func main() {
	if errMsgs := getCLIFlagsErrors(); len(errMsgs) > 0 {
		for _, msg := range errMsgs {
			log.Printf("ERROR: %s\n", msg)
		}

		log.Printf("Run %v --help to see the available cli flags.", os.Args[0])
		os.Exit(1)
	}

	server := fmt.Sprintf("%s:%d", *bootstrapServerFlag, *serverPortFlag)
	consumerId := fmt.Sprintf("kafka-consumer-%d", time.Now().UnixMilli())

	config := &kafka.ConfigMap{
		"bootstrap.servers":                   server,
		"group.id":                            consumerId,
		"client.id":                           consumerId,
		"security.protocol":                   "SSL",
		"ssl.key.location":                    *sslKeyLocationFlag,
		"ssl.certificate.location":            *clientCertLocationFileFlag,
		"ssl.ca.location":                     *caCertLocationFileFlag,
		"auto.offset.reset":                   "latest",
		"enable.ssl.certificate.verification": false,
	}

	log.Printf("Starting kafka consumer %v on server %v. Topic %q", consumerId, server, *topicFlag)

	if !*ceModeFlag {
		readRawKafkaMessages(config)
	} else {
		log.Printf("Cloud Events mode enabled. Cloud Event Type regexp filters: %v\n", evTypeRegexpFilters)
		readCloudEvents(config)
	}
}

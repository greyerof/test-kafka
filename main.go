package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServerFlag *string
	topicFlag           *string
	serverPortFlag      *int
	msgKeyFiltersFlag   *string

	sslKeyLocationFlag         *string
	caCertLocationFileFlag     *string
	clientCertLocationFileFlag *string

	regexFilters []*regexp.Regexp
)

func init() {
	bootstrapServerFlag = flag.String("bootstrap-server", "", "boot-strap server name/ip")
	serverPortFlag = flag.Int("port", 9093, "bootstrap server port, defaulted to 9093 if not provided")
	topicFlag = flag.String("topic", "", "kafka topic to consume")
	msgKeyFiltersFlag = flag.String("kafka-key", "", "comma separated list of kafka key filter regexes. Only messages with matching keys will be shown.")

	sslKeyLocationFlag = flag.String("ssl-key-file", "", "client.key file location for SSL authentication")
	caCertLocationFileFlag = flag.String("ca-crt-file", "", "ca.crt file location for SSL authentication")
	clientCertLocationFileFlag = flag.String("client-crt-file", "", "client.crt file location for SSL authentication")

	flag.Parse()
}

func getKeysFilterRegex(msgKeyFlag string) error {
	filters := strings.Split(msgKeyFlag, ",")

	for _, filter := range filters {
		r, err := regexp.Compile(filter)
		if err != nil {
			return fmt.Errorf("failed to compile regex for key filter %q", filter)
		}

		regexFilters = append(regexFilters, r)
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

	if *msgKeyFiltersFlag != "" {
		if err := getKeysFilterRegex(*msgKeyFiltersFlag); err != nil {
			errors = append(errors, fmt.Sprintf("failed to parse flag --kafka-key: %v", err))
		}
	}

	return errors
}

func shouldPrintKafkaMsg(kafkaKey string) bool {
	if len(regexFilters) == 0 {
		return true
	}

	for _, regexFilter := range regexFilters {
		if regexFilter.MatchString(kafkaKey) {
			return true
		}
	}

	return false
}

type KafkaMsg struct {
	Key     string
	Headers []kafka.Header
	Value   []interface{}
}

func main() {
	if errMsgs := getCLIFlagsErrors(); len(errMsgs) > 0 {
		fmt.Println("ERROR:")
		for _, msg := range errMsgs {
			fmt.Printf("  - %s\n", msg)
		}
		os.Exit(0)
	}

	server := fmt.Sprintf("%s:%d", *bootstrapServerFlag, *serverPortFlag)

	config := &kafka.ConfigMap{
		"bootstrap.servers":                   server,
		"group.id":                            "foo2",
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

	// fmt.Printf("Polling kafka messages from topic %q, server: %q\n", *topicFlag, server)
	// if l := len(regexFilters); l > 0 {
	// 	fmt.Printf("Key filters: %d (%v)\n", l, *msgKeyFiltersFlag)
	// }
	for {
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			if shouldPrintKafkaMsg(string(e.Key)) {

				// fmt.Printf("%v - Message recieved on topic %v, Key: %v\n", time.Now(), e.TopicPartition, string(e.Key))
				// headers := fmt.Sprint(e.Headers)
				// value := string(e.Value)

				// marshalledHeaders, err := json.MarshalIndent(e.Headers, "", "  ")
				// if err != nil {
				// 	fmt.Printf("ERROR: Failed to marshal headers: %v\n", err)
				// } else {
				// 	headers = string(marshalledHeaders)
				// }

				// unmarshalledValue := []interface{}{}
				// err = json.Unmarshal(e.Value, &unmarshalledValue)
				// if err != nil {
				// 	fmt.Printf("ERROR: Failed to unmarshal value: %v", err)
				// } else {
				// 	// Marshal it again!
				// 	marshalledValue, err := json.MarshalIndent(unmarshalledValue, "", "  ")
				// 	if err != nil {
				// 		fmt.Printf("ERROR: Failed to marshal value: %v", err)
				// 	} else {
				// 		value = string(marshalledValue)
				// 	}
				// }

				// fmt.Printf("Headers:\n%v\n", headers)
				// fmt.Printf("Value:\n%v\n", value)

				kafkaMsg := KafkaMsg{
					Headers: e.Headers,
					Key:     string(e.Key),
				}

				err = json.Unmarshal([]byte(string(e.Value)), &kafkaMsg.Value)
				if err != nil {
					fmt.Printf("ERROR: Failed to unmarshal value: %v\n", err)
					break
				}

				s, err := json.MarshalIndent(kafkaMsg, "", "  ")
				if err != nil {
					fmt.Printf("ERROR: failed to marshal kafka msg: %v\n", err)
				}

				fmt.Println(string(s))
			}
		case kafka.Error:
			fmt.Printf("ERROR: failed reading kafka events: %v\n", e)
			os.Exit(0)
		default:
		}
	}
}

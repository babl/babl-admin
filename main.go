package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/larskluge/babl-server/kafka"
)

const (
	Broker    = "v5.babl.sh:9092"
	Topic     = "logs.raw"
	Partition = 0
	LastN     = 1000
	Cols      = 4
	Padding   = 1
)

var (
	Warning = regexp.MustCompile("(?i)warn|overloaded")
	Error   = regexp.MustCompile("(?i)error|panic|fail")

	ColW = make([]int, Cols-1)
)

type Msg struct {
	Hostname      string          `json:"_HOSTNAME"`
	Unit          string          `json:"_SYSTEMD_UNIT"`
	ContainerName string          `json:"CONTAINER_NAME"`
	MessageRaw    json.RawMessage `json:"MESSAGE"`
	Message       string
}

func main() {
	client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	check(err)
	defer consumer.Close()

	offsetNewest, err := client.GetOffset(Topic, Partition, sarama.OffsetNewest)
	check(err)
	offsetOldest, err := client.GetOffset(Topic, Partition, sarama.OffsetOldest)
	check(err)

	offset := offsetNewest - LastN
	if offset < 0 || offset < offsetOldest {
		offset = offsetOldest
	}

	cp, err := consumer.ConsumePartition(Topic, Partition, offset)
	check(err)
	defer cp.Close()

	for msg := range cp.Messages() {
		var m Msg
		err := json.Unmarshal(msg.Value, &m)
		check(err)

		// MESSAGE can be a string or []byte which represents a string; bug in journald/kafka-manager somehow
		var s string
		err = json.Unmarshal(m.MessageRaw, &s)
		if err == nil {
			m.Message = s
		} else {
			var n []byte
			err = json.Unmarshal(m.MessageRaw, &n)
			check(err)
			m.Message = string(n)
		}

		log(logLevel(m), m.Hostname, AppName(m), m.Message)
	}
}

func log(entries ...string) {
	if Cols != len(entries) {
		panic("Adjust Cols")
	}
	for i, entry := range entries {
		w := 0

		// do not adjust col width for last col
		if i < Cols-1 {
			if len(entry) > ColW[i] {
				ColW[i] = len(entry)
			}
			w = ColW[i] + Padding
		}

		fmt.Printf("%-"+strconv.Itoa(w)+"s", entry)
	}
	fmt.Println()
}

func AppName(m Msg) string {
	app := strings.TrimSuffix(m.Unit, ".service")
	if app == "docker" {
		app = m.ContainerName

		// strip instance id
		r := regexp.MustCompile(`^([^\.]+\.\d+)\.\w+$`)
		matches := r.FindStringSubmatch(app)
		if matches != nil {
			app = matches[1]
		}
	}
	return app
}

func logLevel(m Msg) string {
	if Error.MatchString(m.Message) {
		return "ERRO"
	} else if Warning.MatchString(m.Message) {
		return "WARN"
	} else {
		return "INFO"
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

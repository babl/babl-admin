package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	"regexp"
	"strconv"
	"strings"
)

//Warning log level

//Broker url
const (
	Topic     = "logs.raw"
	Partition = 0
	LastN     = 1000
	Cols      = 3
	Padding   = 1
)

var (
	Warning = regexp.MustCompile("(?i)warn|overloaded|timeout|lost")
	Error   = regexp.MustCompile("(?i)error|panic|fail|killed|exception")

	ColW = make([]int, Cols-1)
)

//Msg structure
type Msg struct {
	Hostname         string          `json:"_HOSTNAME"`
	SystemdUnit      string          `json:"_SYSTEMD_UNIT"`
	SyslogIdentifier string          `json:"SYSLOG_IDENTIFIER"`
	ContainerName    string          `json:"CONTAINER_NAME"`
	MessageRaw       json.RawMessage `json:"MESSAGE"`
	Message          string
}

func ParseRaw() {
	client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	Check(err)
	defer consumer.Close()

	offsetNewest, err := client.GetOffset(Topic, Partition, sarama.OffsetNewest)
	Check(err)
	offsetOldest, err := client.GetOffset(Topic, Partition, sarama.OffsetOldest)
	Check(err)

	offset := offsetNewest - LastN
	if offset < 0 || offset < offsetOldest {
		offset = offsetOldest
	}

	cp, err := consumer.ConsumePartition(Topic, Partition, offset)
	Check(err)
	defer cp.Close()

	for msg := range cp.Messages() {
		parseMessage(msg)
	}
}

func parseMessage(msg *sarama.ConsumerMessage) {
	var m Msg
	err := json.Unmarshal(msg.Value, &m)
	Check(err)

	// MESSAGE can be a string or []byte which represents a string; bug in journald/kafka-manager somehow
	var s string
	err = json.Unmarshal(m.MessageRaw, &s)
	if err == nil {
		m.Message = s
	} else {
		var n []byte
		err = json.Unmarshal(m.MessageRaw, &n)
		Check(err)
		m.Message = string(n)
	}
	level := logLevel(m)
	switch level {
	case 'E':
		color.Set(color.FgRed)
	case 'W':
		color.Set(color.FgYellow)
	}
	logRaw(m.Hostname, AppName(m), m.Message)
	color.Unset()
}

func AppName(m Msg) string {
	if m.SyslogIdentifier != "" {
		return m.SyslogIdentifier
	}
	app := strings.TrimSuffix(m.SystemdUnit, ".service")
	if app == "docker" {
		app = m.ContainerName

		// strip instance id
		r := regexp.MustCompile(`^([^\.]+\.\d+)\.\w+$`)
		matches := r.FindStringSubmatch(app)
		if matches != nil {
			app = matches[1]
		}
	} else if strings.HasPrefix(app, "sshd@") {
		app = "sshd"
	}
	return app
}

func logRaw(entries ...string) {
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

func logLevel(m Msg) rune {
	if Error.MatchString(m.Message) {
		return 'E'
	} else if Warning.MatchString(m.Message) {
		return 'W'
	} else {
		return 'I'
	}
}

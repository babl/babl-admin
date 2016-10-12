package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/larskluge/babl-server/kafka"
)

//Broker url
const (
	Topic     = "logs.raw"
	Partition = 0
	LastN     = 1000
	Cols      = 3
	Padding   = 1
)

//Warning log level
var (
	Warning = regexp.MustCompile("(?i)warn|overloaded|timeout|lost")
	Error   = regexp.MustCompile("(?i)error|panic|fail|killed|exception")

	ColW = make([]int, Cols-1)

	flagCluster = flag.String("c", "", "Cluster to connect to, e.g. v5, c2")
	flagDeploy  = flag.String("deploy", "", "Module to deploy, e.g. larskluge/string-upcase")
	flagVersion = flag.String("version", "v0", "Module Version to deploy, e.g. v17")
	flagMemory  = flag.Int("mem", 16, "Memory allowance")
	flagMonitor = flag.String("monitor", "", "cluster stats (lag)")

	Cluster        string
	ClusterAddr    string
	Broker         string
	BurrowEndpoint string
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

func main() {
	flag.Parse()

	Cluster = *flagCluster

	if Cluster == "" {
		fmt.Println("Please specify which cluster to connect to, e.g. -c v5")
		os.Exit(1)
	}

	ClusterAddr = Cluster + ".babl.sh"
	Broker = ClusterAddr + ":9092"
	BurrowEndpoint = "http://" + ClusterAddr + ":8000/v2/kafka"

	if *flagDeploy != "" {
		Deploy(*flagDeploy, *flagVersion, *flagMemory)
		return
	}

	if *flagMonitor == "lag" {
		l := Lag{interval: 1}
		if status, msg := l.ping(); status == true {
			clusters := l.getCluster()
			for _, c := range clusters {
				l.start(c)
			}
		} else {
			fmt.Println("Sorry Cant do!", msg)
		}
		return
	}

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

		level := logLevel(m)
		switch level {
		case 'E':
			color.Set(color.FgRed)
		case 'W':
			color.Set(color.FgYellow)
		}
		log(m.Hostname, AppName(m), m.Message)
		color.Unset()
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

//AppName...
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

func logLevel(m Msg) rune {
	if Error.MatchString(m.Message) {
		return 'E'
	} else if Warning.MatchString(m.Message) {
		return 'W'
	} else {
		return 'I'
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

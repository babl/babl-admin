package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/fatih/color"
	"github.com/larskluge/babl-server/kafka"
)

//Broker url
const (
	Broker = "v5.babl.sh:9092"
	// Topic  = "logs.raw"
	Topic     = "babl.larskluge.RenderWebsite.IO"
	Partition = 0
	LastN     = 1000
	Cols      = 3
	Padding   = 1
)

//Warning log level
var (
	Warning = regexp.MustCompile("(?i)warn|overloaded")
	Error   = regexp.MustCompile("(?i)error|panic|fail|killed|exception")

	ColW = make([]int, Cols-1)

	flagDeploy  = flag.String("deploy", "", "Module to deploy, e.g. larskluge/string-upcase")
	flagVersion = flag.String("version", "v0", "Module Version to deploy, e.g. v17")
	flagMemory  = flag.Int("mem", 16, "Memory allowance")
	flagMonitor = flag.String("monitor", "", "cluster stats (lag)")
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

	// client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	// defer client.Close()

	// consumer, err := sarama.NewConsumerFromClient(client)
	// check(err)
	// defer consumer.Close()

	// offsetNewest, err := client.GetOffset(Topic, Partition, sarama.OffsetNewest)
	// check(err)
	// offsetOldest, err := client.GetOffset(Topic, Partition, sarama.OffsetOldest)
	// check(err)

	// offset := offsetNewest - LastN
	// if offset < 0 || offset < offsetOldest {
	// 	offset = offsetOldest
	// }

	// cp, err := consumer.ConsumePartition(Topic, Partition, offset)
	// check(err)
	// defer cp.Close()

	// for msg := range cp.Messages() {
	// 	// parseRaw(msg)
	// 	// parseTopic(msg)
	// }
	// client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	clientgroup := kafka.NewClientGroup([]string{Broker}, "babl-admin", true)
	defer (*clientgroup).Close()
	parseGroup(clientgroup, []Topic)
}

// func log(entries ...string) {
// 	if Cols != len(entries) {
// 		panic("Adjust Cols")
// 	}
// 	for i, entry := range entries {
// 		w := 0

// 		// do not adjust col width for last col
// 		if i < Cols-1 {
// 			if len(entry) > ColW[i] {
// 				ColW[i] = len(entry)
// 			}
// 			w = ColW[i] + Padding
// 		}

// 		fmt.Printf("%-"+strconv.Itoa(w)+"s", entry)
// 	}
// 	fmt.Println()
// }

func parseGroup(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go kafka.ConsumeGroup(clientgroup, topics, ch)

	for {
		log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		rid := SplitLast(data.Key, ".")
		async := false
		res := "error"
		var msg []byte
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			_, async = in.Env["BABL_ASYNC"]
			delete(in.Env, "BABL_ASYNC") // worker needs to process job synchronously
			if len(in.Env) == 0 {
				in.Env = map[string]string{}
			}
			in.Env["BABL_RID"] = rid
			out, err := IO(in, MaxKafkaMessageSize)
			Check(err)
			if out.Exitcode == 0 {
				res = "success"
			}
			msg, err = proto.Marshal(out)
			Check(err)
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			out, err := Ping(in)
			Check(err)
			res = "success"
			msg, err = proto.Marshal(out)
			Check(err)
		}
		data.Processed <- res
	}
}

func parseTopic(msg *sarama.ConsumerMessage) {
	data := *kafka.ConsumerData{Key: string(msg.Key), Value: msg.Value}
	logrus.WithFields(logrus.Fields{"topic": Topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value": string(data.Value), "value size": len(data.Value), "rid": data.Key}).Info("New Message Received")
}

func parseRaw(msg *sarama.ConsumerMessage) {
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

//AppName extraction func...
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

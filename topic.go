package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"regexp"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"gopkg.in/bsm/sarama-cluster.v2"
)

var regexSup = regexp.MustCompile("^supervisor.*")
var regexText = regexp.MustCompile("^text/plain.*")

func ParseTopic(Topics []string) {
	wait_here_forever := make(chan bool)
	log.SetLevel(log.DebugLevel)

	client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	topics, err := client.Topics()
	Check(err)
	f := func(topic string) bool { return regexSup.MatchString(topic) }
	supTopics := filter(topics, f)
	client.Close()

	clientgroup := kafka.NewClientGroup([]string{Broker}, "babl-admin", true)
	defer (*clientgroup).Close()
	go parseGroup(clientgroup, Topics)
	go parseSupervisors(clientgroup, supTopics)
	<-wait_here_forever
}

func ConsumeGroup(client *cluster.Client, topics []string, ch chan *kafka.ConsumerData) {
	group := "babl-admin_"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	group += fmt.Sprint(r.Uint32())

	log.WithFields(log.Fields{"topics": topics, "group": group, "offset": "newest"}).Info("Consuming Groups")

	consumer, err := cluster.NewConsumerFromClient(client, group, topics)
	Check(err)
	defer consumer.Close()

	go consumeErrors(consumer)
	go consumeNotifications(consumer)

	for msg := range consumer.Messages() {

		data := kafka.ConsumerData{Topic: msg.Topic, Key: string(msg.Key), Value: msg.Value, Processed: make(chan string, 1)}
		ch <- &data                  //SEND
		metadata := <-data.Processed //LISTEN
		consumer.MarkOffset(msg, metadata)
	}
	log.Println("ConsumerGroups: Done consuming topic/groups", topics)
}

func consumeErrors(consumer *cluster.Consumer) {
	for err := range consumer.Errors() {
		log.WithFields(log.Fields{"error": err.Error()}).Warn("ConsumeGroup: Error")
	}
}

func consumeNotifications(consumer *cluster.Consumer) {
	for note := range consumer.Notifications() {
		log.WithFields(log.Fields{"rebalanced": note}).Info("ConsumeGroup: Notification")
	}
}

func parseGroup(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go ConsumeGroup(clientgroup, topics, ch)
	var stdin string
	for {
		data, _ := <-ch //LISTEN

		rid := SplitLast(data.Key, ".")
		res := "error"
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)

			if regexText.MatchString(http.DetectContentType(in.Stdin)) {
				stdin = string(in.Stdin)

			} else {
				stdin = http.DetectContentType(in.Stdin)
			}

			logIoData(rid, data.Topic, len(in.Stdin), in.Env, in.PayloadUrl, stdin)
			res = "success"
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			logPingData(rid, data.Topic)
			res = "success"
		}
		data.Processed <- res //SEND
	}
}
func parseSupervisors(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)

	go ConsumeGroup(clientgroup, topics, ch)
	for {
		data, _ := <-ch //LISTEN
		var stdout, stderr string
		rid := SplitLast(data.Key, ".")
		in := &pbm.BinReply{}
		err := proto.Unmarshal(data.Value, in)
		Check(err)

		if regexText.MatchString(http.DetectContentType(in.Stdout)) {
			stdout = string(in.Stdout)

		} else {
			stdout = http.DetectContentType(in.Stdout)
		}

		if regexText.MatchString(http.DetectContentType(in.Stderr)) {
			stderr = string(in.Stderr)
		} else {
			stderr = http.DetectContentType(in.Stderr)
		}

		logSupervisorData(rid, data.Topic, len(in.Stdout), in.Exitcode, in.PayloadUrl, stdout, stderr)

		data.Processed <- "success" // SEND
	}
}

func logIoData(rid, topic string, size_in int, env interface{}, payload_url string, stdin string) {
	fmt.Printf("RID:%-7s%-42s IN__LEN:%-14d ENV:%v\tPAYLOAD_URL:%s IN:%s\n", rid, topic, size_in, env, payload_url, stdin)
}

func logPingData(rid, topic string) {
	fmt.Printf("RID:%-7s%-42s\t%s\n", rid, topic, "PING")
}

func logSupervisorData(rid, topic string, size_out int, exit_code int32, payload_url string, out string, err string) {
	if exit_code != 0 {
		color.Set(color.FgRed)
	}
	fmt.Printf("RID:%-7s%-42s OUT_LEN:%-14d \tEXIT_CODE: %d\tPAYLOAD_URL: %s (OUT:%s , ERR:%s)\n", rid, topic, size_out, exit_code, payload_url, out, err)
	color.Unset()
}

func filter(s []string, fn func(string) bool) []string {
	var r []string // == nil
	for _, v := range s {
		if fn(v) {
			r = append(r, v)
		}
	}
	return r
}

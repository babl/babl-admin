package main

import (
	"fmt"
	"math/rand"
	"os"
	"text/tabwriter"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"gopkg.in/bsm/sarama-cluster.v2"
)

func ParseTopic(Topic []string) {
	wait_here_forever := make(chan bool)
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.DebugLevel)
	clientgroup := kafka.NewClientGroup([]string{Broker}, "babl-admin", true)
	defer (*clientgroup).Close()
	go parseGroup(clientgroup, Topic)
	go parseSupervisors(clientgroup, []string{"supervisor.a4b4bcae9c39"})
	<-wait_here_forever
}

func GetTopics() {
	//#todo
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
		// rid := SplitLast(data.Key, ".")
		// log.WithFields(log.Fields{"topics": topics, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value": (data.Value), "value size": len(data.Value), "rid": rid}).Info("Message")
		// fmt.Println("MSG->", "T:", msg.Topic, "RID:", rid, "SIZE:", len(data.Value), "P/OFFSET", msg.Partition, "/", msg.Offset)
		ch <- &data
		metadata := <-data.Processed
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

func parseSupervisor(clientgroup *cluster.Client, topics []string) {

}

func parseGroup(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go ConsumeGroup(clientgroup, topics, ch)
	for {
		// log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch

		// log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		rid := SplitLast(data.Key, ".")
		res := "error"
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			logData(rid, data.Topic, len(in.Stdin), in.Env, in.PayloadUrl)
			DataWriter.Flush()
			res = "success"
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			fmt.Println(data.Topic, "-> PING", in)
			res = "success"
		}
		data.Processed <- res
	}
}
func parseSupervisors(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go ConsumeGroup(clientgroup, topics, ch)
	for {
		// log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch

		rid := SplitLast(data.Key, ".")
		in := &pbm.BinReply{}
		err := proto.Unmarshal(data.Value, in)
		Check(err)
		supervisorData(rid, data.Topic, string(in.Stdout), string(in.Stderr), in.Exitcode, in.PayloadUrl)
		DataWriter.Flush()
		data.Processed <- "success"
		// log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		// res := "error"
		// method := SplitLast(data.Topic, ".")
		// switch method {
		// case "IO":
		// 	in := &pbm.BinRequest{}
		// 	err := proto.Unmarshal(data.Value, in)
		// 	Check(err)
		// 	logData(rid, data.Topic, len(in.Stdin), in.Env, in.PayloadUrl)
		// 	DataWriter.Flush()
		// 	res = "success"
		// case "Ping":
		// 	in := &pbm.Empty{}
		// 	err := proto.Unmarshal(data.Value, in)
		// 	Check(err)
		// 	fmt.Println(data.Topic, "-> PING", in)
		// 	res = "success"
		// }

	}
}

var DataWriter = tabwriter.NewWriter(os.Stdout, 0, 100, 10, ' ', 0)

func logData(rid, topic string, size int, env interface{}, payload_url string) {
	fmt.Fprintf(DataWriter, "RID:%s %s\tPAYLOAD_LEN:%d\tENV:%v\tPAYLOAD_URL:%s\n", rid, topic, size, env, payload_url)
}
func supervisorData(rid, topic string, out string, err string, exit_code int32, payload_url string) {
	fmt.Fprintf(DataWriter, "RID:%s %s\tOUT:%s\tERR:%s\tEXIT_CODE: %d\tPAYLOAD_URL: %s\n", rid, topic, out, err, exit_code, payload_url)
}

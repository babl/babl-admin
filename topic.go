package main

import (
	_ "fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"gopkg.in/bsm/sarama-cluster.v2"
	_ "strings"
)

func ParseTopic(Topic string) {
	clientgroup := kafka.NewClientGroup([]string{Broker}, "babl-admin", true)
	defer (*clientgroup).Close()
	//[]string{"babl.larskluge.StringUpcase.IO", "babl.larskluge.StringUpcase.Ping"}
	parseGroup(clientgroup, []string{Topic})
}

func ConsumeGroup(client *cluster.Client, topics []string, ch chan *kafka.ConsumerData) {
	group := "babl-admin-inspector"
	log.WithFields(log.Fields{"topics": topics, "group": group, "offset": "newest"}).Info("Consuming Groups")

	consumer, err := cluster.NewConsumerFromClient(client, group, topics)
	Check(err)
	defer consumer.Close()

	go consumeErrors(consumer)
	go consumeNotifications(consumer)

	for msg := range consumer.Messages() {
		data := kafka.ConsumerData{Topic: msg.Topic, Key: string(msg.Key), Value: msg.Value, Processed: make(chan string, 1)}
		rid := SplitLast(data.Key, ".")
		log.WithFields(log.Fields{"topics": topics, "group": group, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value), "rid": rid}).Info("New Group Message Received")
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

func parseGroup(clientgroup *cluster.Client, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go ConsumeGroup(clientgroup, topics, ch)

	for {
		log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		rid := SplitLast(data.Key, ".")

		res := "error"
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			log.WithFields(log.Fields{"rid": rid, "data": in}).Debug("IO:")
			res = "success"
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			log.WithFields(log.Fields{"rid": rid, "data": in}).Debug("PING:")
			res = "success"
		}
		data.Processed <- res
	}
}

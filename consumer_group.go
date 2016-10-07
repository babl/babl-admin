package kafka

import (
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
	"gopkg.in/bsm/sarama-cluster.v2"
)

// ConsumeGroup Kafka Sarama/Cluster Group Consumer
func ConsumeGroup(client *cluster.Client, topics []string, ch chan *ConsumerData) {
	group := ConsumeGroupName(topics)
	log.WithFields(log.Fields{"topics": topics, "group": group, "offset": "newest"}).Info("Consuming Groups")

	consumer, err := cluster.NewConsumerFromClient(client, group, topics)
	Check(err)
	defer consumer.Close()

	go consumeErrors(consumer)
	go consumeNotifications(consumer)

	for msg := range consumer.Messages() {
		data := ConsumerData{Topic: msg.Topic, Key: string(msg.Key), Value: msg.Value, Processed: make(chan string, 1)}
		rid := SplitLast(data.Key, ".")
		log.WithFields(log.Fields{"topics": topics, "group": group, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value), "rid": rid}).Info("New Group Message Received")
		ch <- &data
		metadata := <-data.Processed
		consumer.MarkOffset(msg, metadata)
	}
	log.Println("ConsumerGroups: Done consuming topic/groups", topics)
}

// ConsumeGroupName unified way to generate a group name
func ConsumeGroupName(topics []string) string {
	sort.Strings(topics)
	return "group." + strings.Join(topics, "|")
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

package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/fatih/color"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	"os/exec"
	_ "regexp"
	_ "strconv"
	_ "strings"
	"time"
)

//Warning log level

//Broker url
const (
	TopicEvents = "logs.events"
)

type Event struct {
	Status string `json:"status"`
	ID     string `json:"id"`
	From   string `json:"from"`
	Type   string `json:"Type"`
	Action string `json:"Action"`
	Actor  struct {
		ID         string `json:"ID"`
		Attributes struct {
			ComDockerSwarmNodeID      string `json:"com.docker.swarm.node.id"`
			ComDockerSwarmServiceID   string `json:"com.docker.swarm.service.id"`
			ComDockerSwarmServiceName string `json:"com.docker.swarm.service.name"`
			ComDockerSwarmTask        string `json:"com.docker.swarm.task"`
			ComDockerSwarmTaskID      string `json:"com.docker.swarm.task.id"`
			ComDockerSwarmTaskName    string `json:"com.docker.swarm.task.name"`
			Image                     string `json:"image"`
			Name                      string `json:"name"`
		} `json:"Attributes"`
	} `json:"Actor"`
	Time     int   `json:"time"`
	TimeNano int64 `json:"timeNano"`
}

func ParseEvents() {
	client := *kafka.NewClient([]string{Broker}, "babl-admin", true)
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	Check(err)
	defer consumer.Close()

	offsetNewest, err := client.GetOffset(TopicEvents, 0, sarama.OffsetNewest)
	Check(err)

	cp, err := consumer.ConsumePartition(TopicEvents, 0, offsetNewest)
	Check(err)
	defer cp.Close()

	for msg := range cp.Messages() {
		parseEvent(msg)
	}
}

func parseEvent(msg *sarama.ConsumerMessage) {
	var m Event
	err := json.Unmarshal(msg.Value, &m)
	Check(err)
	// fmt.Println("events->", m.Type, m.From, m.Action, m.Actor.Attributes.ComDockerSwarmTaskName)
	if m.Type == "container" && (m.Status == "start" || m.Status == "die" || m.Status == "oom") {
		tm := time.Unix(int64(m.Time), 0)
		str := fmt.Sprintf("%s --> %s at %s", m.Actor.Attributes.ComDockerSwarmTaskName, m.Status, tm)
		fmt.Println(str)
		c := fmt.Sprintf("echo '%s' | babl -async -c production.babl.sh:4445 babl/events -e EVENT=babl:error", str)
		fmt.Println("out", c)
		_, err := exec.Command("bash", []string{"-c", c}...).Output()
		Check(err)
	}

}

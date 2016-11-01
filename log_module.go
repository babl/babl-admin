package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
)

const TIME_INTERVAL = 30 //value in seconds to warn about queueing
const TIMEOUT = 60       // timeout for queuing error message

//Warning log level
type responses struct {
	channels map[string]chan string
	mux      sync.Mutex
}

var (
	SupervisorSent = regexp.MustCompile(".*message sent.*IO")
	ModuleReceived = regexp.MustCompile(".*New Group Message Received.*")
	resp           responses
)

func ParseModule() {
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

	resp = responses{channels: make(map[string]chan string)}

	for msg := range cp.Messages() {
		parseFilterMessage(msg)
	}
}

func parseFilterMessage(msg *sarama.ConsumerMessage) {
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

	rid := getAttr("rid", m.Message)

	if rid != "" && SupervisorSent.MatchString(m.Message) {
		topic := getAttr("topic", m.Message)
		partition := getAttr("partition", m.Message)
		offset := getAttr("offset", m.Message)
		// fmt.Println(topic, " -> track message ->", m.Message)
		go trackMessage(rid, topic, partition, offset)
	}

	if rid != "" && ModuleReceived.MatchString(m.Message) {
		go replyMessage(rid, m.Message, AppName(m))
	}

}

func getAttr(atr string, msg string) string {
	var res string
	re := fmt.Sprintf(`"%s":([".a-zA-Z\d]+)`, atr)

	r := regexp.MustCompile(re)
	matches := r.FindStringSubmatch(msg)
	if matches != nil {
		res = matches[1]
	} else {
		res = ""
	}
	return res
}

func trackMessage(rid, topic, partition, offset string) error {
	timeCheck := TIME_INTERVAL
	channel := getRidChannel(rid)
	// fmt.Println("TRACKING:", channel, rid)
	issues := false
	for {
		select {
		case data := <-channel:
			if issues {
				color.Set(color.FgGreen)
			}
			fmt.Println(rid, "->", data)
			color.Unset()

			return nil
		case <-time.After(time.Duration(timeCheck) * time.Second):
			if timeCheck >= TIMEOUT {
				color.Set(color.FgRed)
				fmt.Println(rid, "XXXXXXX->", "MESSAGE ENQUEUED FOR 1 MIN @ ->", topic, partition, offset)
				color.Unset()
				return nil
			}

			color.Set(color.FgYellow)
			fmt.Println(rid, "X->", topic, timeCheck, "seconds in queue!!!", partition, offset)
			color.Unset()

			timeCheck = timeCheck + TIME_INTERVAL
			issues = true
			continue
		}
	}
}

func getRidChannel(rid string) chan string {
	resp.mux.Lock()
	channel, ok := resp.channels[rid]

	resp.mux.Unlock()
	if ok {
		return channel
	} else {
		resp.mux.Lock()
		resp.channels[rid] = make(chan string, 1)
		resp.mux.Unlock()
		return resp.channels[rid]
	}
}

func replyMessage(rid string, msg string, app string) {
	channel := getRidChannel(rid)
	partition := getAttr("partition", msg)
	offset := getAttr("offset", msg)
	reply := fmt.Sprintf("%s, [%s,%s]", app, partition, offset)
	// fmt.Println("reply message ->", msg)
	channel <- reply
	close(channel)
}

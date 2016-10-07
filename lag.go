package main

import (
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
)

//Lag struct
type Lag struct {
	interval int // value in seconds
}

//PartitionT struct
type PartitionT struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Status    string `json:"status"`
	Start     struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"start"`
	End struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"end"`
}

//Consumer struct
type Consumer struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Status  struct {
		Cluster        string       `json:"cluster"`
		Group          string       `json:"group"`
		Status         string       `json:"status"`
		Complete       bool         `json:"complete"`
		Partitions     []PartitionT `json:"partitions"`
		PartitionCount int          `json:"partition_count"`
		Maxlag         struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Status    string `json:"status"`
			Start     struct {
				Offset    int   `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
			} `json:"start"`
			End struct {
				Offset    int   `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
			} `json:"end"`
		} `json:"maxlag"`
		Totallag int `json:"totallag"`
	} `json:"status"`
	Request struct {
		URL     string `json:"url"`
		Host    string `json:"host"`
		Cluster string `json:"cluster"`
		Group   string `json:"group"`
		Topic   string `json:"topic"`
	} `json:"request"`
}

//Burrow Endpoint url
const (
	BurrowEndpoint            = "http://v5.babl.sh:8000/v2/kafka"
	BurrowEndpointConsumer    = "http://v5.babl.sh:8000/v2/kafka/%s/consumer"
	BurrowEndpointConsumerLag = "http://v5.babl.sh:8000/v2/kafka/%s/consumer/%s/lag"
)

func getJSON(url string) map[string]interface{} {
	resp, err := http.Get(url)
	check(err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	// fmt.Println(string(body))
	check(err)
	var obj map[string]interface{}
	err = json.Unmarshal(body, &obj)
	check(err)
	return obj
}

func (l *Lag) ping() (bool, interface{}) {
	resp := getJSON(BurrowEndpoint)
	return resp["error"] == false, resp["message"]
}

func (l *Lag) getCluster() []string {
	var clusters []string
	resp := getJSON(BurrowEndpoint)
	if rec, ok := resp["clusters"].([]interface{}); ok {
		for _, val := range rec {
			clusters = append(clusters, val.(string))
		}
	}
	return clusters
}

func (l *Lag) getConsumers(cluster string) []string {
	var consumers []string
	url := fmt.Sprintf(BurrowEndpointConsumer, cluster)
	resp := getJSON(url)
	if rec, ok := resp["consumers"].([]interface{}); ok {
		for _, val := range rec {
			consumers = append(consumers, val.(string))
		}
	}
	return consumers
}

func (l *Lag) getLag(cluster string, consumer string) Consumer {
	var c Consumer
	url := fmt.Sprintf(BurrowEndpointConsumerLag, cluster, consumer)
	resp, err := http.Get(url)
	check(err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	check(err)
	err = json.Unmarshal(body, &c)
	check(err)
	return c
}

func groupPartionStatus(partitionCount int, partitions []PartitionT) string {
	ok := 0
	stop := 0
	for _, p := range partitions {
		if p.Status == "OK" {
			ok++
		}
		if p.Status == "STOP" {
			stop++
		}
	}
	return fmt.Sprintf("%d [OK:%d,STOP:%d]", partitionCount, ok, stop)
}

func render(data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Group", "Complete", "Status", "Partions", "Lag"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.AppendBulk(data) // Add Bulk Data
	table.Render()
}

func (l *Lag) start(cluster string) {
	fmt.Println("CLUSTER:", cluster)
	consumers := l.getConsumers(cluster)
	sort.Strings(consumers)
	var data [][]string
	for _, c := range consumers {
		consumer := l.getLag(cluster, c)
		line := []string{
			consumer.Status.Group,
			strconv.FormatBool(consumer.Status.Complete),
			consumer.Status.Status,
			groupPartionStatus(consumer.Status.PartitionCount, consumer.Status.Partitions),
			strconv.Itoa(consumer.Status.Totallag)}
		data = append(data, line)
	}
	render(data)
}

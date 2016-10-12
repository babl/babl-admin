package main

import (
	"flag"
	"fmt"
)

//Broker url
const (
	Broker = "v5.babl.sh:9092"
)

var (
	flagDeploy  = flag.String("deploy", "", "Module to deploy, e.g. larskluge/string-upcase")
	flagVersion = flag.String("version", "v0", "Module Version to deploy, e.g. v17")
	flagMemory  = flag.Int("mem", 16, "Memory allowance")
	flagMonitor = flag.String("monitor", "", "cluster stats (lag)")
	flagTopic   = flag.String("topic", "", "topic to inspect")
)

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

	if *flagTopic != "" {
		ParseTopic(*flagTopic)
	} else {
		ParseRaw()
	}
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

// }

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

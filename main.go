package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	flagCluster = flag.String("c", "", "Cluster to connect to, e.g. v5, c2")
	flagDeploy  = flag.String("deploy", "", "Module to deploy, e.g. larskluge/string-upcase")
	flagVersion = flag.String("version", "v0", "Module Version to deploy, e.g. v17")
	flagMemory  = flag.Int("mem", 16, "Memory allowance")
	flagMonitor = flag.String("monitor", "", "cluster stats (lag)")
	flagTopic   = flag.String("topic", "", "topic to inspect")

	Cluster        string
	ClusterAddr    string
	Broker         string
	BurrowEndpoint string
)

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

	if *flagTopic != "" {
		ParseTopic(*flagTopic)
	} else {
		ParseRaw()
	}
}

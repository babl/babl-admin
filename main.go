package main

import (
	"flag"
	"fmt"
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

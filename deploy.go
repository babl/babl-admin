package main

import (
	"fmt"
	"strings"
)

const (
	deployTemplate = "docker service create --name %s --label=module --network=modules -p 4444 --log-driver=journald --reserve-memory=%dM --limit-memory=%dM -e BABL_MODULE=%s -e BABL_MODULE_VERSION=%s -e BABL_COMMAND=/bin/app -e BABL_KAFKA_BROKERS=$CLUSTER_ADDR:9092 -e BABL_STORAGE=$CLUSTER_ADDR:4443 -e BABL_ENDPOINT=$CLUSTER_ADDR:4445 registry.babl.sh/%s:%s"
)

func Deploy(module, version string, mem int) {
	serviceName := strings.Replace(module, "/", "--", 1)
	fmt.Printf(deployTemplate+"\n", serviceName, mem, mem, module, version, module, version)
}

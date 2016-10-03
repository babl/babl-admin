package main

import (
	"fmt"
	"strings"
)

func Deploy(module, version string, mem int) {
	serviceName := strings.Replace(module, "/", "--", 1)
	fmt.Printf("docker service create --name %s --log-driver=journald --reserve-memory=%dM --limit-memory=%dM -e BABL_MODULE=%s -e BABL_MODULE_VERSION=%s -e BABL_COMMAND=/bin/app -e BABL_KAFKA_BROKERS=v5.babl.sh:9092 -e BABL_STORAGE=v5.babl.sh:4443 registry.babl.sh/%s:%s\n",
		serviceName, mem, mem, module, version, module, version)
}

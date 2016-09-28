package main

import (
	"testing"
)

func TestAppNameForDockerContainerWithId(t *testing.T) {
	m := Msg{Unit: "docker.service", ContainerName: "kafka-manager.1.5fnjl2omeamnxtmtb5k9hhg9o"}
	actual := AppName(m)
	expected := "kafka-manager.1"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

func TestAppNameForServiceName(t *testing.T) {
	m := Msg{Unit: "etcd2.service"}
	actual := AppName(m)
	expected := "etcd2"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

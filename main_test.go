package main

import (
	"testing"
)

func TestAppNameForDockerContainerWithId(t *testing.T) {
	m := Msg{SystemdUnit: "docker.service", ContainerName: "kafka-manager.1.5fnjl2omeamnxtmtb5k9hhg9o"}
	actual := AppName(m)
	expected := "kafka-manager.1"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

func TestAppNameForServiceName(t *testing.T) {
	m := Msg{SystemdUnit: "etcd2.service"}
	actual := AppName(m)
	expected := "etcd2"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

func TestAppNameForSshd(t *testing.T) {
	m := Msg{SystemdUnit: "sshd@24-10.19.0.5:8422-85.179.173.138:56805"}
	actual := AppName(m)
	expected := "sshd"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

func TestAppNameForSyslogIdentifier(t *testing.T) {
	m := Msg{SyslogIdentifier: "kernel"}
	actual := AppName(m)
	expected := "kernel"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

func TestGroupPartionStatus(t *testing.T) {
	partitionCount := 10
	partitions := []PartitionT{{Status: "OK"}, {Status: "STOP"}}
	actual := groupPartionStatus(partitionCount, partitions)
	expected := "10 [OK:1,STOP:1]"
	if expected != actual {
		t.Errorf("config mismatch: want %s; got %s", expected, actual)
	}
}

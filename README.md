Currently only streams all cluster log messages for v5.

```sh
go install

babl-admin -c v5 // raw log
babl-admin -c v5 -monitor lag // consumer groups lag

```

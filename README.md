# kafkasort

Before run this program, make sure you have run a kafka server like this [introduction](https://kafka.apache.org/quickstart)

## build

```shell
$ go build
```

## run

```shell
$ ./Kafka -h
Usage of ./Kafka:
  -parts int
        Split number, means handle (total / parts) number everytime when merge sort (default 100)
  -total int
        Total messages number (default 50000000)
$ ./Kafka --total 1000 --parts 2
```



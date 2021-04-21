package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "github.com/Shopify/sarama"

var totalNum = 0
var maxPartNum = 0
var maxSplitMsgNum = 0

const partionNums = 2
const addressConnect = "localhost:9092"
const sendBuffer = 64 * 1024

var partNumMap map[string]map[string]int

/*
id: int32
name: length 10-15, english character only
address: length 15-20, numbers, characters, space
Continent: const value
*/
type dataSchema struct {
	id        int32
	name      string
	address   string
	Continent string
}

func (schema dataSchema) String() string {
	return fmt.Sprint(schema.id, ",", schema.name, ",", schema.address, ",", schema.Continent)
}

func NewDataSchema(str string) *dataSchema {
	parts := strings.Split(str, ",")
	id, _ := strconv.Atoi(parts[0])
	return &dataSchema{int32(id), parts[1], parts[2], parts[3]}
}

type randomDataSchema struct {
	generator *rand.Rand
}

func NewRandomDataSchema() *randomDataSchema {
	p := new(randomDataSchema)
	p.generator = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return p
}

func (random *randomDataSchema) Generate() *dataSchema {
	characters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	addresses := "0123456789" + characters + " "
	continents := []string{"North America", "Asia", "South America", "Europe", "Africa", "Australia"}
	data := &dataSchema{}
	data.id = random.generator.Int31()

	totalLen := 10 + random.generator.Intn(6)
	randBytes := make([]byte, totalLen)
	for i := 0; i < totalLen; i++ {
		randBytes[i] = characters[random.generator.Intn(len(characters))]
	}
	data.name = string(randBytes)

	totalLen = 15 + random.generator.Intn(6)
	randBytes = make([]byte, totalLen)
	for i := 0; i < totalLen; i++ {
		randBytes[i] = addresses[random.generator.Intn(len(addresses))]
	}
	data.address = string(randBytes)

	data.Continent = continents[random.generator.Intn(len(continents))]
	return data
}

func CreateTopic(broker *sarama.Broker, topic string, nums int32) error {
	topicDetails := make(map[string]*sarama.TopicDetail)
	details := &sarama.TopicDetail{NumPartitions: nums, ReplicationFactor: 1}
	topicDetails[topic] = details
	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 30,
		TopicDetails: topicDetails,
	}
	rsp, err := broker.CreateTopics(&request)
	if err != nil {
		return err
	}
	topicErr, ok := rsp.TopicErrors[topic]
	if !ok || topicErr.Err == sarama.ErrNoError || topicErr.Err == sarama.ErrTopicAlreadyExists {
		return nil
	}
	return errors.New("create topicerror: " + topicErr.Error())
}

func NewBroker(config *sarama.Config) (*sarama.Broker, error) {
	broker := sarama.NewBroker(addressConnect)
	err := broker.Open(config)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	_, err = broker.Connected()
	if err != nil {
		broker.Close()
		return nil, err
	}

	return broker, err
}

type ConsumerIter struct {
	pc    sarama.PartitionConsumer
	total int
}
func (consumer *ConsumerIter) Close() {
	consumer.pc.Close()
}

func (consumer *ConsumerIter) Remain() int {
	if !consumer.HasNext() {
		return 0
	}
	return consumer.total
}

func (consumer *ConsumerIter) HasNext() bool {
	return consumer.total > 0
}

func (consumer *ConsumerIter) NextMsg() *sarama.ConsumerMessage {
	consumer.total--
	return <-consumer.pc.Messages()
}

func NewConsumerIter(pc sarama.PartitionConsumer, total int) *ConsumerIter {
	return &ConsumerIter{pc, total}
}

func CreateTopicAndProducer(broker *sarama.Broker, config *sarama.Config, topic string) (sarama.Client, sarama.SyncProducer, error) {
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	client, err := sarama.NewClient([]string{addressConnect}, config)
	if err != nil {
		return nil, nil, err
	}

	err = CreateTopic(broker, topic, partionNums)
	if err != nil {
		return nil, nil, err
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, nil, err
	}
	return client, producer, nil
}

func ProduceSortedSubTopic(broker *sarama.Broker, config *sarama.Config, topic string, subTopic string, sources []*dataSchema, size int) {
	t1 := time.Now()
	if partNumMap[topic] == nil {
		partNumMap[topic] = make(map[string]int)
	}
	partNumMap[topic][subTopic] = size

	divide := int(math.Ceil(float64(size) / partionNums))
	var wg sync.WaitGroup
	for i := 0; i < size; i += divide {
		wg.Add(1)
		go func(i, divide, size int) {
			defer wg.Done()
			client, producer, err := CreateTopicAndProducer(broker, config, subTopic)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer client.Close()
			defer producer.Close()
			part := i / divide
			msgs := make([]*sarama.ProducerMessage, sendBuffer)
			msgIndex := 0
			for j := i; (j < i+divide) && j < size; j++ {
				msg := sarama.ProducerMessage{Topic: subTopic, Partition: int32(part), Key: nil, Value: sarama.StringEncoder(sources[j].String())}
				msgs[msgIndex] = &msg
				msgIndex++
				if msgIndex == len(msgs) {
					msgIndex = 0
					err = producer.SendMessages(msgs)
					if err != nil {
						fmt.Println("send sorted messages:", err)
						return
					}
				}
			}
			if msgIndex != 0 {
				err = producer.SendMessages(msgs[:msgIndex])
				if err != nil {
					fmt.Println("send sorted messages:", err)
					return
				}
			}
		}(i, divide, size)
	}
	wg.Wait()
	t2 := time.Now()
	fmt.Printf("Send to subTopic %s with %d messages past %.2fs\n\n", subTopic, size, t2.Sub(t1).Seconds())
}

func ProduceSource(broker *sarama.Broker, config *sarama.Config, topic string) error {
	tProduce1 := time.Now()

	size := totalNum
	divide := int(math.Ceil(float64(size) / partionNums))
	var wgSend sync.WaitGroup
	for i := 0; i < size; i += divide {
		wgSend.Add(1)
		go func(i int) {
			//defer wgSend.Done()
			client, producer, err := CreateTopicAndProducer(broker, config, topic)
			if err != nil {
				fmt.Println(err)
				return
			}
			wgSend.Done()
			defer client.Close()
			defer producer.Close()
			part := i / divide
			randomData := NewRandomDataSchema()
			// unsafe.Sizeof(sarama.ProducerMessage) is 160
			msgs := make([]*sarama.ProducerMessage, sendBuffer * 2)
			msgIndex := 0
			for j := i; (j < i + divide) && j < size; j++ {
				msg := sarama.ProducerMessage{Topic: topic, Partition: int32(part), Key: nil, Value: sarama.StringEncoder(randomData.Generate().String())}
				msgs[msgIndex] = &msg
				msgIndex++
				if msgIndex == len(msgs) {
					msgIndex = 0
					err = producer.SendMessages(msgs)
					if err != nil {
						fmt.Println("send source messages:", err)
						return
					}
				}
			}
			if msgIndex != 0 {
				err = producer.SendMessages(msgs[:msgIndex])
				if err != nil {
					fmt.Println("send source messages:", err)
					return
				}
			}
		}(i)
	}
	wgSend.Wait()
	tProduce2 := time.Now()
	fmt.Printf("Generate topic %s with %d messages past %.2fs\n\n", topic, size, tProduce2.Sub(tProduce1).Seconds())
	return nil
}

func GetTopicConsume(config *sarama.Config, topic string, size int) (sarama.Client, sarama.Consumer, []*ConsumerIter, error) {
	client, err := sarama.NewClient([]string{addressConnect}, config)
	if err != nil {
		return nil, nil, nil, err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, nil, nil, err
	}

	partions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, nil, nil, err
	}
	consumerIters := make([]*ConsumerIter, len(partions))

	pSize := len(partions)
	divide := int(math.Ceil(float64(size) / partionNums))
	for partionIndex, partion := range partions {
		pc, err := consumer.ConsumePartition(topic, partion, sarama.OffsetOldest)
		if err != nil {
			fmt.Println(err)
			return nil, nil, nil, err
		}
		var total int
		if partionIndex == (pSize - 1) {
			total = size - partionIndex * divide
		} else {
			total = divide
		}
		consumerIters[partionIndex] = NewConsumerIter(pc, total)
	}
	return client, consumer, consumerIters, nil
}

func SortAndProduce(topic string, broker *sarama.Broker, config *sarama.Config, curTopicIndex int, sources []*dataSchema, size int) {
	sort1 := time.Now()
	subTopic := topic + strconv.Itoa(curTopicIndex)
	if topic == "id" {
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].id < sources[j].id
		})
	} else if topic == "name" {
		sort.Slice(sources, func(i, j int) bool {
			return strings.ToLower(sources[i].name) < strings.ToLower(sources[j].name)
		})
	} else {
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].Continent < sources[j].Continent
		})
	}
	sort2 := time.Now()
	fmt.Printf("Sort subTopic %s with %d messages past %.2fs\n\n", subTopic, size, sort2.Sub(sort1).Seconds())
	ProduceSortedSubTopic(broker, config, topic, subTopic, sources, size)
}

func SplitAndProduceSubTopic(broker *sarama.Broker, config *sarama.Config, topic string) error {
	client, producer, consumerIters, err := GetTopicConsume(config, topic, totalNum)
	if err != nil {
		return err
	}
	defer client.Close()
	defer producer.Close()

	remainTotal := totalNum
	size := maxSplitMsgNum
	sources := make([]*dataSchema, size)
	sourceLen := len(sources)
	curTopicIndex := 0
	for remainTotal > 0 {
		t1 := time.Now()
		fmt.Println("remain messages: ", remainTotal)

		var wg sync.WaitGroup
		curIndex := 0
		divideSize := int(math.Ceil(float64(size) / float64(len(consumerIters))))
		for _, consume := range consumerIters {
			var rightIndex int
			remain := consume.Remain()
			if remain <= 0 {
				continue
			}
			if remain + curIndex == sourceLen || remain < divideSize {
				rightIndex = curIndex + remain
			} else {
				rightIndex = curIndex + divideSize
			}
			rightIndex = int(math.Min(float64(sourceLen), float64(rightIndex)))
			wg.Add(1)
			go func(consume *ConsumerIter, leftIndex, rightIndex int) {
				defer wg.Done()
				for i := leftIndex; i <= rightIndex; i++ {
					if !consume.HasNext() {
						fmt.Println("Impossible consume")
						return
					}
					sources[i] = NewDataSchema(string(consume.NextMsg().Value))
				}
			}(consume, curIndex, rightIndex - 1)
			curIndex = rightIndex
		}
		wg.Wait()
		remainTotal -= curIndex

		t2 := time.Now()
		fmt.Printf("Consume topic %s past %.2fs\n\n", topic, t2.Sub(t1).Seconds())

		SortAndProduce("id", broker, config, curTopicIndex, sources, curIndex)
		SortAndProduce("name", broker, config, curTopicIndex, sources, curIndex)
		SortAndProduce("continent", broker, config, curTopicIndex, sources, curIndex)

		curTopicIndex++
	}
	for _, consume := range consumerIters {
		consume.Close()
	}
	return nil
}

func MergeSplitedSubTopic(broker *sarama.Broker, config *sarama.Config) {
	merge1 := time.Now()

	var wgTopic sync.WaitGroup
	for topic, subMap := range partNumMap {
		wgTopic.Add(1)

		func (topic string, subMap map[string]int) {
			defer wgTopic.Done()
			tTopic1 := time.Now()
			fmt.Printf("Beginning to produce topic: %s \n", topic)

			client, producer, err := CreateTopicAndProducer(broker, config, topic)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer client.Close()
			defer producer.Close()

			size := totalNum
			divide := int(math.Ceil(float64(size) / partionNums))
			curProduceIndex := 0

			mapSize := len(subMap)
			subConsumeIters := make([][]*ConsumerIter, mapSize)
			consumeIndex := 0

			for subTopic, size := range subMap {
				_, _, subConsumeIter, err := GetTopicConsume(config, subTopic, size)
				if err != nil {
					fmt.Println(err)
					return
				}
				subConsumeIters[consumeIndex] = subConsumeIter
				consumeIndex++
			}
			partionIndexes := make([]int, mapSize)
			curVal := make([]*dataSchema, mapSize)

			curMinIndex := -1
			msgs := make([]*sarama.ProducerMessage, sendBuffer)
			msgIndex := 0
			for {
				for mapIndex := 0; mapIndex < mapSize; mapIndex++ {
					if curVal[mapIndex] == nil && partionIndexes[mapIndex] < len(subConsumeIters[mapIndex]) && subConsumeIters[mapIndex][partionIndexes[mapIndex]].HasNext() {
						curVal[mapIndex] = NewDataSchema(string(subConsumeIters[mapIndex][partionIndexes[mapIndex]].NextMsg().Value))
						if !subConsumeIters[mapIndex][partionIndexes[mapIndex]].HasNext() {
							subConsumeIters[mapIndex][partionIndexes[mapIndex]].Close()
							partionIndexes[mapIndex]++
						}
					}

					if curVal[mapIndex] == nil {
						continue
					}
					if curMinIndex == -1 {
						curMinIndex = mapIndex
						continue
					}
					if curVal[mapIndex].id < curVal[curMinIndex].id {
						curMinIndex = mapIndex
					}
				}
				if curMinIndex == -1 {
					if msgIndex != 0 {
						err = producer.SendMessages(msgs[:msgIndex])
						if err != nil {
							fmt.Printf("produce topic error: %s:%s", topic, err.Error())
							return
						}
					}
					break
				}
				msg := sarama.ProducerMessage{Topic: topic, Partition: int32(curProduceIndex / divide), Key: nil, Value: sarama.StringEncoder(curVal[curMinIndex].String())}
				msgs[msgIndex] = &msg
				msgIndex++
				if msgIndex == len(msgs) {
					msgIndex = 0
					err = producer.SendMessages(msgs)
					if err != nil {
						fmt.Printf("produce topic error: %s:%s", topic, err.Error())
						return
					}
				}
				curProduceIndex++
				curVal[curMinIndex] = nil
				curMinIndex = -1
			}
			tTopic2 := time.Now()
			fmt.Printf("Generate and produce topic: %s with %d messages past %.2fs\n", topic, curProduceIndex, tTopic2.Sub(tTopic1).Seconds())
		}(topic, subMap)
	}
	wgTopic.Wait()
	merge2 := time.Now()
	fmt.Printf("merge total messages past %.2fs\n\n", merge2.Sub(merge1).Seconds())
}

func main() {
	total1 := time.Now()
	partNumMap = make(map[string]map[string]int)

	flag.IntVar(&totalNum, "total", 50000000, "Total messages number")
	flag.IntVar(&maxPartNum, "parts", 100, "Split number, means handle (total / parts) number everytime when merge sort")
	flag.Parse()
	maxSplitMsgNum = totalNum / maxPartNum
	fmt.Printf("total messages: %d, handle %d everytime\n", totalNum, maxSplitMsgNum)

	config := sarama.NewConfig()
	config.ChannelBufferSize = 1024
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	broker, err := NewBroker(config)
	if err != nil {
		fmt.Println(err)
		return
	}

	topic := "source"
	err = ProduceSource(broker, config, topic)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = SplitAndProduceSubTopic(broker, config, topic)
	if err != nil {
		fmt.Println(err)
		return
	}

	MergeSplitedSubTopic(broker, config)

	total2 := time.Now()
	fmt.Printf("total time past %.2fs\n\n", total2.Sub(total1).Seconds())
}

// @File: main1
// @Date: 2021/1/27 19:23
// @Author: 安红豆
// @Description: API的调用示例
package main

import (
	"github.com/ormissia/go-rabbitmq"
	"log"
	"strconv"
)

const MQURL = "amqp://guest:guest@127.0.0.1:5672/"

func main() {

	//example12()
	//example3()
	//example4()
	example5()
}

//eg. Work Mode 1/2
func example12() {
	rabbitmq1 := gorabbitmq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq1.Destroy()
	rabbitmq2 := gorabbitmq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq2.Destroy()

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs := rabbitmq2.Consume()
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 3
func example3() {
	rabbitmq1 := gorabbitmq.NewRabbitMqSubscription("exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	rabbitmq2 := gorabbitmq.NewRabbitMqSubscription("exchange.example3", MQURL)
	defer rabbitmq2.Destroy()

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs := rabbitmq2.Consume()
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 4
func example4() {
	rabbitmq1 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	rabbitmq2 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	rabbitmq3 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.two", MQURL)
	defer rabbitmq3.Destroy()

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs := rabbitmq2.Consume()
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs := rabbitmq3.Consume()
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 5
func example5() {
	rabbitmq1 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	rabbitmq2 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	rabbitmq3 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.two", MQURL)
	defer rabbitmq3.Destroy()
	rabbitmq4 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.*", MQURL)
	defer rabbitmq4.Destroy()

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs := rabbitmq2.Consume()
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs := rabbitmq3.Consume()
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs := rabbitmq4.Consume()
		for d := range msgs {
			log.Printf("key.*接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

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
	rabbitmq1, err1 := gorabbitmq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := gorabbitmq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err3 := rabbitmq2.Consume()
		if err3 != nil {
			log.Println(err3)
		}
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 3
func example3() {
	rabbitmq1, err1 := gorabbitmq.NewRabbitMqSubscription("exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := gorabbitmq.NewRabbitMqSubscription("exchange.example3", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err3 := rabbitmq2.Consume()
		if err3 != nil {
			log.Println(err3)
		}
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 4
func example4() {
	rabbitmq1, err1 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := gorabbitmq.NewRabbitMqRouting("exchange.example4", "key.two", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err4 := rabbitmq2.Consume()
		if err4 != nil {
			log.Println(err4)
		}
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err5 := rabbitmq3.Consume()
		if err5 != nil {
			log.Println(err5)
		}
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

//eg. Work Mode 5
func example5() {
	rabbitmq1, err1 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.two", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}
	rabbitmq4, err4 := gorabbitmq.NewRabbitMqTopic("exchange.example5", "key.*", MQURL)
	defer rabbitmq4.Destroy()
	if err4 != nil {
		log.Println(err4)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err5 := rabbitmq2.Consume()
		if err5 != nil {
			log.Println(err5)
		}
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err6 := rabbitmq3.Consume()
		if err6 != nil {
			log.Println(err6)
		}
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err7 := rabbitmq4.Consume()
		if err7 != nil {
			log.Println(err7)
		}
		for d := range msgs {
			log.Printf("key.*接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

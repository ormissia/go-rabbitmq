// @File: rabbitmq
// @Date: 2021/1/27 17:30
// @Author: 安红豆
// @Description: 对“github.com/streadway/amqp”进行二次封装，方便调用
package gorabbitmq

import (
	"github.com/streadway/amqp"
	"log"
)

/*
二次封装了RabbitMQ五种模式：
1 Simple模式，最简单最常用的模式，一个消息只能被一个消费者消费
	应用场景: 短信，聊天
2 Work模式，一个消息只能被一个消费者消费
	应用场景: 抢红包，和资源任务调度
3 Publish/Subscribe发布订阅模式，消息被路由投递给多个队列，一个消息被多个消费者获取,生产端不允许指定消费
	应用场景：邮件群发，广告
4 Routing路由模式,一个消息被多个消费者获取，并且消息的目标队列可以被生产者指定
	应用场景: 根据生产者的要求发送给特定的一个或者一批队列发送信息
5 Topic话题模式,一个消息被多个消息获取，消息的目标queue可用BindKey以通配符
	（#:一个或多个词，*：一个词）的方式指定。
*/

//RabbitMQ实例
type RabbitMQ struct {
	conn         *amqp.Connection //连接
	channel      *amqp.Channel    //管道
	ExchangeName string           //交换机名称
	QueueName    string           //队列名称
	Key          string           //Binding Key/Routing Key, Simple模式 几乎用不到
	MqURL        string           //连接信息-amqp://账号:密码@地址:端口号/-amqp://guest:guest@127.0.0.1:5672/
}

//创建一个RabbitMQ实例
func newRabbitMQ(exchangeName, queueName, key, mqUrl string) *RabbitMQ {
	rabbitmq := &RabbitMQ{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		Key:          key,
		MqURL:        mqUrl,
	}
	var err error
	//创建rabbitmq连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqURL)
	rabbitmq.failOnError(err, "Failed to connect to RabbitMQ")
	//
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "Failed to open a channel")

	return rabbitmq
}

//断开channel和connection
func (r *RabbitMQ) Destroy() {
	r.channel.Close()
	r.conn.Close()
	log.Fatalf("%s,%s 关闭了！", r.ExchangeName, r.QueueName)
}

//错误处理
func (r *RabbitMQ) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

/*
1 Simple模式，最简单最常用的模式
2 Work模式，一个消息只能被一个消费者消费
*/
//创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewRabbitMQSimple(queueName, mqUrl string) *RabbitMQ {
	rabbitmq := newRabbitMQ("", queueName, "", mqUrl)
	return rabbitmq
}

//直接模式,生产者.
func (r *RabbitMQ) PublishSimple(message string) {
	//1 申请队列，如不存在，则自动创建之，存在，则路过。
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare a queue")

	//2 发送消息到队列中
	err = r.channel.Publish(
		r.ExchangeName,
		r.QueueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	r.failOnError(err, "Failed to publish an message:"+message)
}

//直接模式，消费者
func (r *RabbitMQ) ConsumeSimple() <-chan amqp.Delivery {
	//1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		false, //是否持久化
		false, //是否自动删除
		false, //是否具有排他性
		false, //是否阻塞处理
		nil,   //额外的属性
	)
	r.failOnError(err, "Failed to declare a queue")

	//2 接收消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",   //用来区分多个消费者
		true, //是否自动应答,告诉我已经消费完了
		false,
		false, //若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false, //消费队列是否设计阻塞
		nil,
	)
	r.failOnError(err, "Failed to register a consumer")

	return msgs
}

/*
3 Publish/Subscribe发布订阅模式
*/

//获取订阅模式下的rabbitmq的实例
func NewRabbitMqSubscription(exchangeName, mqUrl string) *RabbitMQ {
	//创建rabbitmq实例
	rabbitmq := newRabbitMQ(exchangeName, "", "", mqUrl)
	return rabbitmq
}

//订阅模式发布消息
func (r *RabbitMQ) PublishSubscription(message string) {
	//1 尝试连接交换机
	err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout", //这里一定要设计为"fanout"也就是广播类型。
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange")

	//2 发送消息
	err = r.channel.Publish(
		r.ExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	r.failOnError(err, "Failed to publish an message:"+message)
}

//订阅模式消费者
func (r *RabbitMQ) ConsumeSubscription() <-chan amqp.Delivery {
	//1 试探性创建交换机exchange
	err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange")

	//2 试探性创建队列queue
	q, err := r.channel.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "订阅模式消费方法中创建创建队列失败。")

	//3 绑定队列到交换机中
	err = r.channel.QueueBind(
		q.Name,
		"", //在pub/sub模式下key要为空
		r.ExchangeName,
		false,
		nil,
	)

	//4 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	return msgs
}

/*
4 Routing路由模式
*/
func NewRabbitMqRouting(exchangeName, routingKey, mqUrl string) *RabbitMQ {
	rabbitmq := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	return rabbitmq
}

//路由模式发送信息
func (r *RabbitMQ) PublishRouting(message string) {
	//1 尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.ExchangeName,
		//交换机类型 广播类型
		"direct",
		//是否持久化
		true,
		//是否字段删除
		false,
		//true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		//是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange")
	//2 发送信息
	err = r.channel.Publish(
		r.ExchangeName,
		//Binding Key
		r.Key,
		false,
		false,
		amqp.Publishing{
			//类型
			ContentType: "text/plain",
			//消息
			Body: []byte(message),
		})
	r.failOnError(err, "Failed to publish an message:"+message)
}

//路由模式接收信息
func (r *RabbitMQ) ConsumeRouting() <-chan amqp.Delivery {
	//1 尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.ExchangeName,
		//交换机类型 广播类型
		"direct",
		//是否持久化
		true,
		//是否字段删除
		false,
		//true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		//是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange")

	//2 试探性创建队列
	q, err := r.channel.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare a queue")

	//3 绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		//在pub/sub模式下，这里的key要为空
		r.Key,
		r.ExchangeName,
		false,
		nil,
	)
	r.failOnError(err, "Failed to bind a queue")
	//4 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to register a consumer")

	//5 直接把amqp的channel返回给调用者
	return msgs
}

/*
5 Topic话题模式
*/
func NewRabbitMqTopic(exchangeName, routingKey, mqUrl string) *RabbitMQ {
	rabbitmq := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	return rabbitmq
}

//topic模式。生产者。
func (r *RabbitMQ) PublishTopic(message string) {
	//1 尝试创建交换机,这里的kind的类型要改为topic
	err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "topic模式尝试创建exchange失败。")

	//2 发送消息。
	err = r.channel.Publish(
		r.ExchangeName,
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

//topic模式。消费者。"*"表示匹配一个单词。“#”表示匹配多个单词，亦可以是0个。
func (r *RabbitMQ) ConsumeTopic() <-chan amqp.Delivery {
	//1 创建交换机。这里的kind需要是“topic”类型。
	err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true, //这里需要是true
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "topic模式，消费者创建exchange失败。")

	//2 创建队列。这里不用写队列名称。
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "topic模式，消费者创建queue失败。")

	//3 将队列绑定到交换机里。
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.ExchangeName,
		false,
		nil,
	)

	//4 消费消息
	msgs, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	return msgs
}

// @File: rabbitmq
// @Date: 2021/1/27 17:30
// @Author: 安红豆
// @Description: 对“github.com/streadway/amqp”进行二次封装，方便调用
package gorabbitmq

import (
	errors "errors"
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

//定义RabbitMQ实例的接口
//每种RabbitMQ实例都有发布和消费两种功能
type RabbitMQInterface interface {
	Publish(message string) (err error)
	Consume() (consumeChan <-chan amqp.Delivery, err error)
}

//创建一个RabbitMQ实例
func newRabbitMQ(exchangeName, queueName, key, mqUrl string) (rabbitmq *RabbitMQ, err error) {
	rabbitmq = &RabbitMQ{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		Key:          key,
		MqURL:        mqUrl,
	}
	//创建rabbitmq连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqURL)
	if err != nil {
		return nil, err
	}
	//
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	if err != nil {
		return nil, err
	}
	return rabbitmq, nil
}

//断开channel和connection
func (r *RabbitMQ) Destroy() {
	r.channel.Close()
	r.conn.Close()
	log.Printf("%s,%s is closed!!!", r.ExchangeName, r.QueueName)
}

/*
1 Simple模式，最简单最常用的模式
2 Work模式，一个消息只能被一个消费者消费
*/
type RabbitMQSimple struct {
	*RabbitMQ
}

//创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewRabbitMQSimple(queueName, mqUrl string) (rabbitMQSimple *RabbitMQSimple, err error) {
	//判断是否输入必要的信息
	if queueName == "" || mqUrl == "" {
		log.Fatalf("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", queueName, mqUrl)
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ("", queueName, "", mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMQSimple{
		rabbitmq,
	}, nil
}

//直接模式,生产者.
func (r *RabbitMQSimple) Publish(message string) (err error) {
	//1 申请队列，如不存在，则自动创建之，存在，则路过。
	_, err = r.channel.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}
	return nil
}

//直接模式，消费者
func (r *RabbitMQSimple) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	//1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		false, //是否持久化
		false, //是否自动删除
		false, //是否具有排他性
		false, //是否阻塞处理
		nil,   //额外的属性
	)
	if err != nil {
		return nil, err
	}

	//2 接收消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",   //用来区分多个消费者
		true, //是否自动应答,告诉我已经消费完了
		false,
		false, //若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false, //消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}

/*
3 Publish/Subscribe发布订阅模式
*/
type RabbitMqSubscription struct {
	*RabbitMQ
}

//获取订阅模式下的rabbitmq的实例
func NewRabbitMqSubscription(exchangeName, mqUrl string) (rabbitMqSubscription *RabbitMqSubscription, err error) {
	//判断是否输入必要的信息
	if exchangeName == "" || mqUrl == "" {
		log.Fatalf("ExchangeName and mqUrl is required,\nbut exchangeName and mqUrl are %s and %s.", exchangeName, mqUrl)
		return nil, errors.New("ExchangeName and mqUrl is required")
	}
	//创建rabbitmq实例
	rabbitmq, err := newRabbitMQ(exchangeName, "", "", mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMqSubscription{
		rabbitmq,
	}, nil
}

//订阅模式发布消息
func (r *RabbitMqSubscription) Publish(message string) (err error) {
	//1 尝试连接交换机
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout", //这里一定要设计为"fanout"也就是广播类型。
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}

	return nil
}

//订阅模式消费者
func (r *RabbitMqSubscription) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	//1 试探性创建交换机exchange
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//2 试探性创建队列queue
	q, err := r.channel.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//3 绑定队列到交换机中
	err = r.channel.QueueBind(
		q.Name,
		"", //在pub/sub模式下key要为空
		r.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//4 消费消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}

/*
4 Routing路由模式
*/
type RabbitMqRouting struct {
	*RabbitMQ
}

//获取路由模式下的rabbitmq的实例
func NewRabbitMqRouting(exchangeName, routingKey, mqUrl string) (rabbitMqRouting *RabbitMqRouting, err error) {
	//判断是否输入必要的信息
	if exchangeName == "" || routingKey == "" || mqUrl == "" {
		log.Fatalf("ExchangeName, routingKey and mqUrl is required,\nbut exchangeName, routingKey and mqUrl are %s, %s and %s.", exchangeName, routingKey, mqUrl)
		return nil, errors.New("ExchangeName, routingKey and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMqRouting{
		rabbitmq,
	}, nil
}

//路由模式发送信息
func (r *RabbitMqRouting) Publish(message string) (err error) {
	//1 尝试创建交换机，不存在创建
	err = r.channel.ExchangeDeclare(
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
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}

	return nil
}

//路由模式接收信息
func (r *RabbitMqRouting) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	//1 尝试创建交换机，不存在创建
	err = r.channel.ExchangeDeclare(
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
	if err != nil {
		return nil, err
	}

	//2 试探性创建队列
	q, err := r.channel.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//3 绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		//在pub/sub模式下，这里的key要为空
		r.Key,
		r.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//4 消费消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//5 直接把amqp的channel返回给调用者
	return consumeChan, nil
}

/*
5 Topic话题模式
*/
type RabbitMQTopic struct {
	*RabbitMQ
}

//获取话题模式下的rabbitmq的实例
func NewRabbitMqTopic(exchangeName, routingKey, mqUrl string) (rabbitMQTopic *RabbitMQTopic, err error) {
	//判断是否输入必要的信息
	if exchangeName == "" || routingKey == "" || mqUrl == "" {
		log.Fatalf("ExchangeName, routingKey and mqUrl is required,\nbut exchangeName, routingKey and mqUrl are %s and %s.", exchangeName, routingKey, mqUrl)
		return nil, errors.New("ExchangeName, routingKey and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMQTopic{
		rabbitmq,
	}, nil
}

//topic模式。生产者。
func (r *RabbitMQTopic) Publish(message string) (err error) {
	//1 尝试创建交换机,这里的kind的类型要改为topic
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}
	return nil
}

//topic模式。消费者。"*"表示匹配一个单词。“#”表示匹配多个单词，亦可以是0个。
func (r *RabbitMQTopic) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	//1 创建交换机。这里的kind需要是“topic”类型。
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true, //这里需要是true
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//2 创建队列。这里不用写队列名称。
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//3 将队列绑定到交换机里。
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//4 消费消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}

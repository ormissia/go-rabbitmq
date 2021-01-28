# RabbitMQ Go API二次封装

> 对[ RabbitMQ Go API ](https://github.com/streadway/amqp) 的二次封装

[![](https://img.shields.io/badge/RabbitMQ-API-blue)](https://github.com/streadway/amqp)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/ormissia/go-rabbitmq.svg)](https://pkg.go.dev/github.com/ormissia/go-rabbitmq)

#### 二次封装了RabbitMQ五种模式：
1. Simple模式，最简单的模式，一个消息只能被一个消费者消费。 
   应用场景: 短信，聊天
   
   ![](https://ormissia-blog.oss-cn-qingdao.aliyuncs.com/image-hosting/RabbitMQ%E7%AE%80%E5%8D%95%E6%A8%A1%E5%BC%8F.png)
   
2. Work模式，一个消息只能被一个消费者消费。 
   应用场景: 抢红包，和资源任务调度
   
   ![](https://ormissia-blog.oss-cn-qingdao.aliyuncs.com/image-hosting/RabbitMQ%E5%B7%A5%E4%BD%9C%E6%A8%A1%E5%BC%8F.png)
   
3. Publish/Subscribe发布订阅模式，消息被路由投递给多个队列，一个消息被多个消费者获取,生产端不允许指定消费。 
   应用场景：邮件群发，广告
   
   ![](https://ormissia-blog.oss-cn-qingdao.aliyuncs.com/image-hosting/RabbitMQ%E5%8F%91%E5%B8%83%E8%AE%A2%E9%98%85%E6%A8%A1%E5%BC%8F.png)
   
4. Routing路由模式,一个消息被多个消费者获取，并且消息的目标队列可以被生产者指定。 
   应用场景: 根据生产者的要求发送给特定的一个或者一批队列发送信息
   
   ![](https://ormissia-blog.oss-cn-qingdao.aliyuncs.com/image-hosting/RabbitMQ%E8%B7%AF%E7%94%B1%E6%A8%A1%E5%BC%8F.png)
   
5. Topic话题模式,一个消息被多个消息获取，消息的目标queue可用BindKey以通配符。 
   （#:一个或多个词，*：一个词）的方式指定
   
   ![](https://ormissia-blog.oss-cn-qingdao.aliyuncs.com/image-hosting/RabbitMQ%E8%AF%9D%E9%A2%98%E6%A8%A1%E5%BC%8F.png)
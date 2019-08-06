package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Co 2 tinh huong:
	// 1. Ca Producer va Consumer phai giong nhau
	// 2. Pushisher co the push len truoc
	// 3. Consumer neu da connect ma stop la Queue se bi delete
	autoDelete := true
	noWait := true
	exclusive := false
	q, err := ch.QueueDeclare(
		"hello",    // name
		false,      // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		noWait,     // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	autoAck := false
	msgs, err := ch.Consume(
		q.Name,  // queue
		"",      // consumer
		autoAck, // auto-ack
		false,   // exclusive
		false,   // no-local
		true,    // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		var d amqp.Delivery
		for d = range msgs {
			log.Printf("Received a message: %s", d.Body)
			// d.Ack(true)
			time.Sleep(time.Millisecond * 5)
		}
		// Logic cho nay sai, vi dieu nay dau co xay ra voi vong for o tren
		// d.Ack(true)
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

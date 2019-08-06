package main

import (
	"fmt"
	"log"
	"strconv"
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
	// 4. Tao ra cai Queue de hung cai message push vao
	autoDelete := true
	noWait := true
	// @todo should search about when "exclusive is true" should be used?
	exclusive := false // Co y nghia ve viec chi cho duy nhat mot connection connect vao cai queue do
	q, err := ch.QueueDeclare(
		"hello",    // name
		false,      // durable => the queue will survive a broker restart
		autoDelete, // autoDelete => delete when unused
		exclusive,  // exclusive
		noWait,     // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")
	ch.QueueBind("hello", "hello", "logs_direct", false, nil)
	// fmt.Println("1111111")
	// noWait2 := true //
	// // Chuong trinh bi stuck o day
	// if err = ch.Confirm(noWait2); err != nil {
	// 	failOnError(err, "Confirm channel is error")
	// }
	// fmt.Println("2222222")

	for i := 0; i < 10000; i++ {
		body := "AAAAA " + strconv.Itoa(i)
		err = ch.Publish(
			"logs_direct", // exchange (default amqp)
			// Chu y cai routing key va cai q.Name that su hem co lien quan
			// Ban chat la luc minh khai bao cai bind cho exchange voi cai queue minh se quyet dinh
			// La giua 2 cai do se lien lac qua cai key gi.
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		time.Sleep(time.Millisecond * 1000)
		fmt.Printf("Mgs %d \n", i)
		failOnError(err, "Failed to publish a message")
	}
}

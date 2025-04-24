// cmd/notification-service/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"kafka-notify/cmd/interfaces"
	"kafka-notify/cmd/producer"
	"kafka-notify/cmd/service"
)

func main() {
    // Command line flags
    brokerType := flag.String("broker", "kafka", "Broker type: kafka or kinesis")
    kafkaBrokers := flag.String("kafka-brokers", "172.19.245.31:9092", "Comma-separated list of Kafka brokers")
    kafkaTopic := flag.String("kafka-topic", "notifications", "Kafka topic")
    awsRegion := flag.String("aws-region", "us-east-1", "AWS region")
    kinesisStream := flag.String("kinesis-stream", "notifications", "Kinesis stream name")
    
    flag.Parse()
    
    var notificationProducer interfaces.NotificationProducer
    var err error
    
    // Initialize the selected producer
    switch *brokerType {
    case "kafka":
        brokers := strings.Split(*kafkaBrokers, ",")
        notificationProducer, err = producer.NewKafkaProducer(brokers, *kafkaTopic)
        if err != nil {
            log.Fatalf("Failed to create Kafka producer: %v", err)
        }
        
    case "kinesis":
        notificationProducer, err = producer.NewKinesisProducer(*awsRegion, *kinesisStream)
        if err != nil {
            log.Fatalf("Failed to create Kinesis producer: %v", err)
        }
        
    default:
        log.Fatalf("Unknown broker type: %s", *brokerType)
    }
    
    // Create notification service
    notificationService := service.NewNotificationService(notificationProducer)
    defer notificationProducer.Close()
    
    // Example: Send system announcement
    err = notificationService.BroadcastSystemAnnouncement(
        "System Update",
        "Our app is undergoing maintenance. Expected downtime: 30 minutes.",
    )
    if err != nil {
        log.Printf("Failed to send system announcement: %v", err)
    }
    
    // Example: Send targeted notification
    err = notificationService.SendTargetedNotification(
        "marketing",
        "Special Offer!",
        "50% off all premium features this weekend!",
        []string{"premium_users", "active_users"},
    )
    if err != nil {
        log.Printf("Failed to send targeted notification: %v", err)
    }
    
    fmt.Println("Notifications sent successfully!")
}
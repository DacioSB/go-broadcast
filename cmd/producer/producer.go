package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type NotificationProducer interface {
    SendNotification(notification Notification) error
    Close() error
}

type Notification struct {
    ID          string    `json:"id"`
    Type        string    `json:"type"`
    Title       string    `json:"title"`
    Message     string    `json:"message"`
    Timestamp   time.Time `json:"timestamp"`
    Targets     []string  `json:"targets"`
}

type KafkaProducer struct {
    producer sarama.SyncProducer
    topic    string
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    
    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        return nil, fmt.Errorf("failed to setup Kafka producer: %w", err)
    }

    return &KafkaProducer{
        producer: producer,
        topic:    topic,
    }, nil
}

// SendNotification sends a notification via Kafka
func (k *KafkaProducer) SendNotification(notification Notification) error {
    // Prepare notification
    notification.ID = generateUniqueID()
    notification.Timestamp = time.Now()

    // Serialize the notification
    notificationJSON, err := json.Marshal(notification)
    if err != nil {
        return fmt.Errorf("failed to marshal notification: %w", err)
    }

    // Create Kafka message
    msg := &sarama.ProducerMessage{
        Topic: k.topic,
        Value: sarama.StringEncoder(notificationJSON),
    }

    // Send message to Kafka
    _, _, err = k.producer.SendMessage(msg)
    if err != nil {
        return fmt.Errorf("failed to send notification via Kafka: %w", err)
    }

    return nil
}

// Helper function to generate unique IDs
func generateUniqueID() string {
    return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (k *KafkaProducer) Close() error {
    return k.producer.Close()
}

type KinesisProducer struct {
    client *kinesis.Kinesis
    stream string
}

// NewKinesisProducer creates a new Kinesis producer
func NewKinesisProducer(region, streamName string) (*KinesisProducer, error) {
    // Create AWS session
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create AWS session: %w", err)
    }

    // Create Kinesis client
    client := kinesis.New(sess)

    return &KinesisProducer{
        client: client,
        stream: streamName,
    }, nil
}

// SendNotification sends a notification via Kinesis
func (k *KinesisProducer) SendNotification(notification Notification) error {
    // Prepare notification
    notification.ID = generateUniqueID()
    notification.Timestamp = time.Now()

    // Serialize the notification
    notificationJSON, err := json.Marshal(notification)
    if err != nil {
        return fmt.Errorf("failed to marshal notification: %w", err)
    }

    // Create Kinesis record
    record := &kinesis.PutRecordInput{
        Data:         notificationJSON,
        PartitionKey: aws.String(notification.ID),
        StreamName:   aws.String(k.stream),
    }

    // Send record to Kinesis
    _, err = k.client.PutRecord(record)
    if err != nil {
        return fmt.Errorf("failed to send notification via Kinesis: %w", err)
    }

    return nil
}

// Close is a no-op for Kinesis producer (AWS SDK manages connections)
func (k *KinesisProducer) Close() error {
    return nil
}

// Notification creation helpers
func createSystemAnnouncement() Notification {
    return Notification{
        Type:     "system",
        Title:    "System Update",
        Message:  "Our app is undergoing maintenance. Expected downtime: 30 minutes.",
        Targets:  []string{"all_users"},
    }
}

func createMarketingPromotion() Notification {
    return Notification{
        Type:     "marketing",
        Title:    "Special Offer!",
        Message:  "50% off all premium features this weekend!",
        Targets:  []string{"premium_users", "active_users"},
    }
}

// Example usage
func main() {
    // Kafka Producer Example
    kafkaProducer, err := NewKafkaProducer(
        []string{"localhost:9092"}, 
        "broadcast-notifications",
    )
    if err != nil {
        log.Fatalf("Failed to create Kafka producer: %v", err)
    }
    defer kafkaProducer.Close()

    // Kinesis Producer Example
    kinesisProducer, err := NewKinesisProducer(
        "us-west-2", 
        "notification-stream",
    )
    if err != nil {
        log.Fatalf("Failed to create Kinesis producer: %v", err)
    }

    // Create sample notifications
    notifications := []Notification{
        createSystemAnnouncement(),
        createMarketingPromotion(),
    }

    // Demonstrate flexible producer usage
    var producer NotificationProducer = kafkaProducer
    // Uncomment the next line to switch to Kinesis
    // producer = kinesisProducer

    // Send notifications
    for _, notification := range notifications {
        err = producer.SendNotification(notification)
        if err != nil {
            log.Printf("Failed to broadcast notification: %v", err)
        }
    }
}
// producer/kafka_producer.go
package producer

import (
	"encoding/json"
	"fmt"
	"kafka-notify/cmd/interfaces"
	"time"

	"github.com/IBM/sarama"
)

// KafkaProducer implements NotificationProducer for Kafka
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
        return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
    }
    
    return &KafkaProducer{
        producer: producer,
        topic:    topic,
    }, nil
}

// Send broadcasts a notification via Kafka
func (k *KafkaProducer) Send(notification interfaces.Notification) error {
    // Set metadata if not present
    if notification.ID == "" {
        notification.ID = fmt.Sprintf("%d", time.Now().UnixNano())
    }
    if notification.Timestamp.IsZero() {
        notification.Timestamp = time.Now()
    }
    
    // Serialize notification
    data, err := json.Marshal(notification)
    if err != nil {
        return fmt.Errorf("failed to marshal notification: %w", err)
    }
    
    // Create and send message
    msg := &sarama.ProducerMessage{
        Topic: k.topic,
        Value: sarama.StringEncoder(data),
    }
    
    _, _, err = k.producer.SendMessage(msg)
    if err != nil {
        return fmt.Errorf("failed to send message: %w", err)
    }
    
    return nil
}

// Close releases resources
func (k *KafkaProducer) Close() error {
    return k.producer.Close()
}
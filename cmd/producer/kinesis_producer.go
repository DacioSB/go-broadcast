// producer/kinesis_producer.go
package producer

import (
	"encoding/json"
	"fmt"
	"kafka-notify/cmd/interfaces"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// KinesisProducer implements NotificationProducer for AWS Kinesis
type KinesisProducer struct {
    client    *kinesis.Kinesis
    streamName string
}

// NewKinesisProducer creates a new Kinesis producer
func NewKinesisProducer(region, streamName string) (*KinesisProducer, error) {
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create AWS session: %w", err)
    }
    
    return &KinesisProducer{
        client:     kinesis.New(sess),
        streamName: streamName,
    }, nil
}

// Send broadcasts a notification via Kinesis
func (k *KinesisProducer) Send(notification interfaces.Notification) error {
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
    
    // Create partition key (could be based on target groups)
    partitionKey := notification.ID
    
    // Put record to Kinesis
    _, err = k.client.PutRecord(&kinesis.PutRecordInput{
        Data:         data,
        StreamName:   aws.String(k.streamName),
        PartitionKey: aws.String(partitionKey),
    })
    
    if err != nil {
        return fmt.Errorf("failed to put record to Kinesis: %w", err)
    }
    
    return nil
}

// Close for Kinesis is a no-op as the AWS SDK handles connection pooling
func (k *KinesisProducer) Close() error {
    // Nothing to close in the Kinesis client
    return nil
}
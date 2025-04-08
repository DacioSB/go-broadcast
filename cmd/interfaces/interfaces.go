// producer/interfaces.go
package interfaces

import "time"

// Notification represents a broadcast message
type Notification struct {
    ID        string    `json:"id"`
    Type      string    `json:"type"`
    Title     string    `json:"title"`
    Message   string    `json:"message"`
    Timestamp time.Time `json:"timestamp"`
    Targets   []string  `json:"targets"`
}

// NotificationProducer defines methods required for sending notifications
type NotificationProducer interface {
    // Send broadcasts a notification
    Send(notification Notification) error
    
    // Close cleans up resources
    Close() error
}
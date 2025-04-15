// service/notification_service.go
package service

import (
	"kafka-notify/cmd/interfaces"
)

type NotificationService struct {
    producer interfaces.NotificationProducer 
}

func NewNotificationService(p interfaces.NotificationProducer) *NotificationService {
    return &NotificationService{
        producer: p,
    }
}

func (s *NotificationService) BroadcastSystemAnnouncement(title, message string) error {
    notification := interfaces.Notification{
        Type:    "system",
        Title:   title,
        Message: message,
        Targets: []string{"all_users"},
    }
    
    return s.producer.Send(notification)
}

func (s *NotificationService) SendTargetedNotification(
    notificationType string,
    title string,
    message string,
    targets []string,
) error {
    notification := interfaces.Notification{ 
        Type:    notificationType,
        Title:   title,
        Message: message,
        Targets: targets,
    }
    
    return s.producer.Send(notification)
}
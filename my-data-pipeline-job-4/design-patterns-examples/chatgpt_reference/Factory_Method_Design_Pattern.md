```mermaid
flowchart TD
    A[Client calls get_notification_factory - notification_type]
    B[Check notification type]
    C[Return concrete NotificationFactory]
    D[Call create_notification on factory]
    E[Concrete Notification object is created]
    F[Client calls notify - message on Notification object]

    A --> B
    B -- "notification_type = email" --> C[EmailNotificationFactory]
    B -- "notification_type = sms" --> C[SMSNotificationFactory]
    B -- "notification_type = push" --> C[PushNotificationFactory]
    C --> D
    D --> E
    E --> F

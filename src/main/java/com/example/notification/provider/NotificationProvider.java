package com.example.notification.provider;

import com.example.notification.domain.Notification;

public interface NotificationProvider {

    /**
     * Delivers the notification to its recipient. Implementations may block while
     * talking to an external system and must throw if delivery fails.
     */
    void send(Notification notification);
}

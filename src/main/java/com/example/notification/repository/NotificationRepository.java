package com.example.notification.repository;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    Optional<Notification> findByIdempotencyKey(String idempotencyKey);

    long countByStatus(NotificationStatus status);

    List<Notification> findByOrderByCreatedAtDesc(Pageable pageable);

    List<Notification> findByStatusOrderByCreatedAtDesc(NotificationStatus status, Pageable pageable);
}

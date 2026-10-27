-- NotificationStatus remains the internal queue lifecycle (PENDING/SENDING/SENT/DEAD).
-- Provider callbacks use delivery_status so a successful hand-off to Brevo is never confused
-- with an inbox delivery, and retries/dead-letter handling retain their existing semantics.
ALTER TABLE notifications
    ADD COLUMN delivery_status varchar(32),
    ADD COLUMN provider_event_at timestamp(6) with time zone,
    ADD COLUMN provider_accepted_at timestamp(6) with time zone,
    ADD COLUMN delivered_at timestamp(6) with time zone;

-- Older SENT rows predate provider callbacks. Their delivery was not confirmed.
UPDATE notifications
   SET status = 'SENDING', delivery_status = 'ACCEPTED',
       provider_accepted_at = coalesce(sent_at, created_at), sent_at = null
 WHERE status = 'SENT';

CREATE TABLE notification_delivery_events (
    id                bigserial                   NOT NULL,
    provider          varchar(32)                 NOT NULL,
    provider_event_id bigint                      NOT NULL,
    notification_id   uuid                        NOT NULL,
    event_type        varchar(64)                 NOT NULL,
    occurred_at       timestamp(6) with time zone NOT NULL,
    reason            varchar(255),
    received_at       timestamp(6) with time zone NOT NULL DEFAULT now(),

    CONSTRAINT notification_delivery_events_pkey PRIMARY KEY (id),
    CONSTRAINT uk_delivery_events_provider_event UNIQUE (provider, provider_event_id),
    CONSTRAINT fk_delivery_events_notification
        FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE RESTRICT
);

CREATE INDEX idx_delivery_events_notification_occurred
    ON notification_delivery_events (notification_id, occurred_at DESC);

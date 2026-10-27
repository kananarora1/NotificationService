CREATE INDEX idx_notifications_awaiting_confirmation_age
    ON notifications (provider_accepted_at)
    WHERE status = 'SENDING' AND provider_accepted_at IS NOT NULL;

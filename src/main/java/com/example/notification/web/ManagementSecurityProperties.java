package com.example.notification.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Access control for operator-only Actuator metrics endpoints. */
@ConfigurationProperties(prefix = "management.security")
public record ManagementSecurityProperties(boolean enabled, String token) {

    public ManagementSecurityProperties {
        token = token == null ? "" : token;
    }
}

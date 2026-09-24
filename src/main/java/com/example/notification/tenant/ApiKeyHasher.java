package com.example.notification.tenant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Hashes API keys for storage and lookup.
 *
 * <p><strong>SHA-256, deliberately — not bcrypt/argon2.</strong> Those are password
 * hashes: they are slow <em>by design</em> (a tunable work factor, ~100ms) to make
 * brute-forcing low-entropy human-chosen passwords impractical. Neither property is
 * wanted here:
 *
 * <ul>
 *   <li>API keys are verified on <em>every</em> request. A 100ms KDF would dominate the
 *       latency of an endpoint whose p99 is a few milliseconds, and would cap throughput
 *       at roughly (threads / 0.1s) requests per second.</li>
 *   <li>API keys are generated, not chosen: 256 bits of {@code SecureRandom}. There is
 *       no dictionary to run and no feasible brute force, so the work factor buys
 *       nothing. The reason to hash at all is that a leaked database dump must not
 *       hand over working credentials.</li>
 *   <li>A per-key salt would defeat the point: lookup is <em>by</em> the hash, on an
 *       index. Salting would force a scan of every tenant row per request. Unsalted is
 *       safe precisely because the input is high-entropy — rainbow tables cover human
 *       passwords, not 256-bit random strings.</li>
 * </ul>
 *
 * <p>The hex encoding matches Postgres's built-in
 * {@code encode(sha256(convert_to(key,'UTF8')),'hex')}, which is what the V3 migration
 * uses to backfill existing rows.
 */
public final class ApiKeyHasher {

    private ApiKeyHasher() {
    }

    /** Returns the lowercase hex SHA-256 digest of {@code apiKey}'s UTF-8 bytes. */
    public static String sha256Hex(String apiKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(apiKey.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JRE; this cannot happen.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}

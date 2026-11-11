package com.jobscheduler.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jobscheduler.job.NewJob;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Fingerprints a request so a reused idempotency key can be told apart from a retry: same key and
 * same body is a retry, same key and a different body is a client bug.
 */
@Component
public class RequestHasher {

    private final ObjectMapper canonical;

    public RequestHasher(ObjectMapper objectMapper) {
        // Sorted map keys, so {"a":1,"b":2} and {"b":2,"a":1} hash the same.
        this.canonical = objectMapper.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public String hash(NewJob request) {
        try {
            byte[] json = canonical.writeValueAsBytes(request);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Could not hash request", e);
        }
    }
}

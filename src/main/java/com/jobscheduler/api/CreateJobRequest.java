package com.jobscheduler.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

public record CreateJobRequest(
        @NotBlank
        @Size(max = 100)
        @Pattern(regexp = "[a-z0-9][a-z0-9._-]*", message = "must be lowercase letters, digits, '.', '_' or '-'")
        String type,
        Map<String, Object> payload,
        /* When to run the job. Omit to run as soon as possible. */
        Instant runAt,
        @Min(1) @Max(20)
        Integer maxAttempts) {
}

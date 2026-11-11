package com.jobscheduler.observability;

import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Spring Boot turns metric export off in tests unless asked; this test needs the real endpoint. */
@AutoConfigureObservability(tracing = false)
class MetricsIntegrationTest extends IntegrationTest {

    @Test
    void jobActivityShowsUpInPrometheusEndpoint() throws Exception {
        UUID ok = submit(uniqueTenant(), """
                {"type": "counting"}
                """);
        UUID bad = submit(uniqueTenant(), """
                {"type": "always-fail", "maxAttempts": 1}
                """);
        awaitStatus(ok, "SUCCEEDED");
        awaitStatus(bad, "FAILED");

        String scrape = mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(scrape)
                .contains("jobs_submitted_total{")
                .contains("jobs_execution_seconds_bucket{")
                .contains("jobs_queue_wait_seconds_bucket{")
                .contains("jobs_dead_lettered_total{")
                .contains("jobs_by_status{")
                .contains("outbox_backlog");
        assertThat(scrape).containsPattern("jobs_execution_seconds_count\\{[^}]*outcome=\"success\"[^}]*type=\"counting\"");
    }
}

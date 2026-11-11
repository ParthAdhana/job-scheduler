package com.jobscheduler.job;

import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobApiIntegrationTest extends IntegrationTest {

    /** Far enough ahead that background workers leave these jobs alone during the test. */
    private static final String LATER = "\"runAt\": \"2099-01-01T00:00:00Z\"";

    @Test
    void createThenFetchJob() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "noop", "payload": {"to": "a@example.com"}, "maxAttempts": 5, %s}
                """.formatted(LATER));

        mvc.perform(get("/jobs/{id}", id).header("X-Tenant-Id", tenant))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.to").value("a@example.com"))
                .andExpect(jsonPath("$.maxAttempts").value(5));
    }

    @Test
    void createReturnsLocationHeader() throws Exception {
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"type": "noop", %s}
                        """.formatted(LATER)))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"));
    }

    @Test
    void jobsAreIsolatedByTenant() throws Exception {
        UUID id = submit("tenant-a", """
                {"type": "noop", %s}
                """.formatted(LATER));

        mvc.perform(get("/jobs/{id}", id).header("X-Tenant-Id", "tenant-b"))
                .andExpect(status().isNotFound());
    }

    @Test
    void cancelPendingJobThenRejectSecondCancel() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "sleep", %s}
                """.formatted(LATER));

        mvc.perform(post("/jobs/{id}/cancel", id).header("X-Tenant-Id", tenant))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mvc.perform(post("/jobs/{id}/cancel", id).header("X-Tenant-Id", tenant))
                .andExpect(status().isConflict());
    }

    @Test
    void listFiltersByStatus() throws Exception {
        String tenant = uniqueTenant();
        submit(tenant, """
                {"type": "noop", %s}
                """.formatted(LATER));
        UUID cancelled = submit(tenant, """
                {"type": "sleep", %s}
                """.formatted(LATER));
        mvc.perform(post("/jobs/{id}/cancel", cancelled).header("X-Tenant-Id", tenant));

        mvc.perform(get("/jobs").param("status", "PENDING").header("X-Tenant-Id", tenant))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalItems").value(1))
                .andExpect(jsonPath("$.items[0].type").value("noop"));
    }

    @Test
    void rejectsInvalidRequest() throws Exception {
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type": "Bad Type!", "maxAttempts": 99}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.type").exists())
                .andExpect(jsonPath("$.errors.maxAttempts").exists());

        mvc.perform(get("/jobs").param("size", "500"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownJobType() throws Exception {
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type": "does-not-exist"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("noop")));
    }
}

package com.jobscheduler.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class JobApiIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void createThenFetchJob() throws Exception {
        String id = createJob("tenant-a", """
                {"type": "send-email", "payload": {"to": "a@example.com"}, "maxAttempts": 5}
                """);

        mvc.perform(get("/jobs/{id}", id).header("X-Tenant-Id", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.to").value("a@example.com"))
                .andExpect(jsonPath("$.maxAttempts").value(5));
    }

    @Test
    void jobsAreIsolatedByTenant() throws Exception {
        String id = createJob("tenant-a", """
                {"type": "send-email"}
                """);

        mvc.perform(get("/jobs/{id}", id).header("X-Tenant-Id", "tenant-b"))
                .andExpect(status().isNotFound());
    }

    @Test
    void cancelPendingJobThenRejectSecondCancel() throws Exception {
        String id = createJob("tenant-c", """
                {"type": "resize-image"}
                """);

        mvc.perform(post("/jobs/{id}/cancel", id).header("X-Tenant-Id", "tenant-c"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mvc.perform(post("/jobs/{id}/cancel", id).header("X-Tenant-Id", "tenant-c"))
                .andExpect(status().isConflict());
    }

    @Test
    void listFiltersByStatus() throws Exception {
        createJob("tenant-d", """
                {"type": "a"}
                """);
        String cancelled = createJob("tenant-d", """
                {"type": "b"}
                """);
        mvc.perform(post("/jobs/{id}/cancel", cancelled).header("X-Tenant-Id", "tenant-d"));

        mvc.perform(get("/jobs").param("status", "PENDING").header("X-Tenant-Id", "tenant-d"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalItems").value(1))
                .andExpect(jsonPath("$.items[0].type").value("a"));
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

    private String createJob(String tenant, String body) throws Exception {
        String response = mvc.perform(post("/jobs")
                        .header("X-Tenant-Id", tenant)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andReturn().getResponse().getContentAsString();
        JsonNode json = objectMapper.readTree(response);
        return json.get("id").asText();
    }
}

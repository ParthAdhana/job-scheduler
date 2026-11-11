package com.jobscheduler.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Base class: the whole app against real Postgres and Kafka, plus helpers for the HTTP API. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Import({TestcontainersConfiguration.class, IntegrationTest.TestHandlers.class})
public abstract class IntegrationTest {

    @Autowired
    protected MockMvc mvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected JdbcClient jdbc;

    @Autowired
    protected CountingHandler countingHandler;

    @TestConfiguration(proxyBeanMethods = false)
    static class TestHandlers {
        @Bean
        CountingHandler countingHandler() {
            return new CountingHandler();
        }
    }

    protected UUID submit(String tenant, String body) throws Exception {
        String response = mvc.perform(post("/jobs")
                        .header("X-Tenant-Id", tenant)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(objectMapper.readTree(response).get("id").asText());
    }

    protected JsonNode fetch(String tenant, UUID id) throws Exception {
        String response = mvc.perform(get("/jobs/{id}", id).header("X-Tenant-Id", tenant))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response);
    }

    protected String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single();
    }

    protected void awaitStatus(UUID id, String expected) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                .until(() -> expected.equals(statusOf(id)));
    }

    protected static String uniqueTenant() {
        return "t-" + UUID.randomUUID().toString().substring(0, 8);
    }
}

package com.jobscheduler.ratelimit;

import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class RateLimitIntegrationTest extends IntegrationTest {

    /** Configured in application-test.yml with a bucket of 3 and a very slow refill. */
    private static final String LIMITED = "limited-tenant";

    @Test
    void tenantOverItsLimitGets429WhileOthersAreUnaffected() throws Exception {
        for (int i = 0; i < 3; i++) {
            submit(LIMITED).andExpect(status().isCreated());
        }

        submit(LIMITED)
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"))
                .andExpect(header().string("X-RateLimit-Remaining", "0"));

        submit(uniqueTenant()).andExpect(status().isCreated());
        // Reads are never limited.
        mvc.perform(get("/jobs").header("X-Tenant-Id", LIMITED)).andExpect(status().isOk());
    }

    private ResultActions submit(String tenant) throws Exception {
        return mvc.perform(post("/jobs").header("X-Tenant-Id", tenant)
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"type": "noop", "runAt": "2099-01-01T00:00:00Z"}
                        """));
    }
}

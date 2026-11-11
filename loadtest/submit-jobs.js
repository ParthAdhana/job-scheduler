// Submits jobs at a fixed arrival rate and checks the API keeps up.
//
//   k6 run loadtest/submit-jobs.js
//   k6 run -e RATE=500 -e DURATION=2m -e TYPE=sleep loadtest/submit-jobs.js
//
// k6 measures the API side (submit latency, errors). End-to-end numbers (how long jobs took to
// finish) come from the database afterwards: see loadtest/report.sql.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 200);           // jobs submitted per second
const DURATION = __ENV.DURATION || '60s';
const TYPE = __ENV.TYPE || 'noop';
// Spread across tenants so the per-tenant rate limit (10/s by default) is not what gets measured.
const TENANTS = Number(__ENV.TENANTS || 100);
// Tag jobs from this run so report.sql can find them.
const RUN_ID = __ENV.RUN_ID || `run-${Date.now()}`;

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250'],
  },
};

export function setup() {
  console.log(`RUN_ID=${RUN_ID}`);
  return { runId: RUN_ID };
}

export default function (data) {
  // A global counter, so tenants rotate evenly across all virtual users.
  const n = exec.scenario.iterationInTest;
  const tenant = `load-${n % TENANTS}`;
  const body = JSON.stringify({
    type: TYPE,
    payload: { runId: data.runId, durationMs: 50 },
    priority: n % 10 === 0 ? 'HIGH' : 'NORMAL',
  });
  const res = http.post(`${BASE_URL}/jobs`, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-Id': tenant,
      // Unique per request: exercises the idempotency path without deduplicating anything.
      'Idempotency-Key': `${data.runId}-${n}`,
    },
  });
  check(res, { 'accepted (201)': (r) => r.status === 201 });
}

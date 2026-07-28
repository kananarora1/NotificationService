import http from 'k6/http';

// Drives readable shapes into the Grafana dashboard. Three tenants, three behaviours:
//   - acme    (limit 60/min): steady, well-behaved, stays under budget  -> mostly 202
//   - globex  (limit 10/min): periodic bursts over budget               -> spikes of 429
//   - initech (limit 5/min):  sustained flood far above budget          -> heavy, constant 429
//
// Run: k6 run k6/load.js            (defaults to http://localhost:8080)
//      BASE_URL=http://localhost:8081 k6 run k6/load.js

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

function notify(apiKey, recipient) {
  http.post(
    `${BASE}/notify`,
    JSON.stringify({ recipient, channel: 'EMAIL', message: 'load test' }),
    { headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey }, tags: { tenant: apiKey } },
  );
}

export const options = {
  scenarios: {
    acme_steady: {
      executor: 'ramping-arrival-rate',
      exec: 'acme',
      startRate: 5,
      timeUnit: '1m',
      preAllocatedVUs: 20,
      maxVUs: 50,
      stages: [
        { target: 30, duration: '1m' },
        { target: 45, duration: '2m' }, // under the 60/min limit -> few/no 429
        { target: 30, duration: '1m' },
      ],
    },
    globex_bursty: {
      executor: 'ramping-arrival-rate',
      exec: 'globex',
      startRate: 2,
      timeUnit: '1m',
      preAllocatedVUs: 20,
      maxVUs: 50,
      stages: [
        { target: 2, duration: '1m' },
        { target: 40, duration: '30s' }, // burst well over 10/min -> 429 spikes
        { target: 2, duration: '1m' },
        { target: 40, duration: '30s' },
        { target: 2, duration: '1m' },
      ],
    },
    initech_flood: {
      executor: 'constant-arrival-rate',
      rate: 100,              // was 2
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 100,
      maxVUs: 300,
      exec: 'initech',
    },
  },
};

export function acme() {
  notify('key-acme', `acme-${__VU}-${__ITER}@example.com`);
}

export function globex() {
  notify('key-globex', `globex-${__VU}-${__ITER}@example.com`);
}

export function initech() {
  notify('key-initech', `initech-${__VU}-${__ITER}@example.com`);
}

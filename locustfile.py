import random
import uuid

from locust import HttpUser, between, task


TICKETS = [
    "Customers cannot sign in after the latest deployment.",
    "The billing page shows an old invoice after refresh.",
    "An urgent checkout outage is blocking several customers.",
]


class TenantUser(HttpUser):
    wait_time = between(0.2, 0.8)

    def on_start(self):
        # Each Locust user registers a different API key so the test creates separate tenant rate-limit buckets.
        response = self.client.post(
            "/register",
            json={"email": f"load-test-{uuid.uuid4()}@example.com"}
        )
        response.raise_for_status()
        self.headers = {"X-API-Key": response.json()["api_key"]}
        self.job_ids = []

    @task(3)
    def submit_job(self):
        with self.client.post(
            "/jobs",
            json={"text": random.choice(TICKETS), "task": "summarize"},
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 202:
                self.job_ids.append(response.json()["job_id"])
                self.job_ids = self.job_ids[-20:]
            elif response.status_code == 429:
                # A 429 is expected after this tenant spends its own bucket; server errors still fail the test.
                response.success()
            else:
                response.failure(f"Unexpected status code: {response.status_code}")

    @task
    def poll_job(self):
        if not self.job_ids:
            return
        with self.client.get(
            f"/jobs/{random.choice(self.job_ids)}",
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 429:
                response.success()
            elif response.status_code != 200:
                response.failure(f"Unexpected status code: {response.status_code}")

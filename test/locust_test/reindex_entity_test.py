from locust import HttpUser, task, constant
import json
import random
import os
from dotenv import load_dotenv

load_dotenv()
with open("ids.json") as f:
    valid_ids = json.load(f)

class GetEntityUser(HttpUser):
    wait_time = constant(0) 
    token = os.getenv("TOKEN")

    @task
    def hit_authenticated_endpoint(self):
        entity_id = random.choice(valid_ids)
        headers = {"Authorization": f"Bearer {self.token}"}
        self.client.put(f"/reindex/{entity_id}", headers=headers)

    
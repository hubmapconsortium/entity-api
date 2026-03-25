# Load Testing `entity-api` with Locust

This directory contains our Locust load-testing setup for `entity-api`. While Locust can simulate complex user journeys, our primary goal here is to stress-test our infrastructure to see how different configuration values affect failure points under sustained load. 

Locust provides excellent real-time visualizations for requests over time, response times, and failure rates, making it easy to observe exactly when our services run out of available connections.

## Prerequisites & Setup

1. **Install Dependencies:** Ensure you have the required packages installed via `requirements.txt`.
   ```bash
   pip install -r requirements.txt
   ```
   *(Note: This installs `locust` and `python-dotenv`)*

2. **Environment Variables:** Create a `.env` file in this directory to store your authorization token.
   ```env
   TOKEN=your_actual_bearer_token_here
   ```

3. **Test Data:** You will need an `ids.json` file in this directory containing an array of valid UUIDs (about 1000 is sufficient). We use real IDs to ensure the API processes the GET requests fully, rather than immediately returning a 404 for a missing ID or a 400 for a malformed one.

## The Locust Task

The current test cases, including the `GetEntityUser` class and the authenticated endpoint tasks, are defined in the local [get_entity_test.py](./get_entity_test.py). 

This script is configured to:
* Use a `constant(0)` wait time to maximize throughput.
* Randomly select a UUID from `ids.json` for each request.
* Inject the required `Authorization` header using your local `.env` file.

## Running the Tests

1. Start the Locust service using the CLI from wherever the locust file is located and give the path to that file:
   ```bash
   locust -f path/to/file
   ```
2. Open your browser and navigate to the Locust GUI: [http://localhost:8089](http://localhost:8089)
3. Configure the test parameters in the UI:
   * **Number of users:** `1000` (Sufficient to force a failure state).
   * **Spawn rate:** `5` (Users added per second).
   * **Host:** `http://localhost:xxxx` (The local URL for `entity-api`).

## Observing Results & Identifying Failures

This setup makes it easy to do a control run with no tweaks to the service and see when we hit 502/504 errors. 

When you start the run, switch to the **Charts** tab. Watch the users ramp up. When we've reached the limit of available `uwsgi` connections, there will be a sharp spike in the red line representing failures. You can switch over to the **Failures** tab to confirm that these are `502` errors. Make any desired tweaks to the service configurations, then run again.

## Benchmarks & Configuration Tweaks

*(Note: "Virtual users" here is simply an abstraction; it is just the easiest consistent metric we have to represent the load on the service).*

**Baseline:**
* Ramping up 5 users/sec with `constant(0)` wait time, the service reliably fails **between 400 and 450 virtual users** on a vm we used for testing; your numbers will likely vary.

**Optimized Configuration:**
By tweaking `uwsgi` and `nginx` settings, we roughly double the amount of time until failure, reliably reaching **just over 800 virtual users**. 

To replicate these results locally, apply the following tweaks:
* **uwsgi:** Set `max-requests` to `2000` and increase the `listen` queue value to `512`.
* **nginx:** Apply the following timeout and buffer parameters to your configuration:

```nginx
# Maximum time nginx will wait to establish a connection to the uwsgi upstream before giving up
uwsgi_connect_timeout 10s;

# Maximum time nginx will wait between successive writes to uwsgi when sending a request
uwsgi_send_timeout    90;

# Maximum time nginx will wait for uwsgi to send a response, preventing premature 502/504 errors on legitimately slow requests
uwsgi_read_timeout    90;

# Size of the buffer used to read the first part of the uwsgi response header
uwsgi_buffer_size     32k;

# Number and size of buffers used for reading the uwsgi response body, reducing disk buffering warnings for larger responses
uwsgi_buffers         4 32k;
```

## Further Resources

For more information on writing complex tasks, custom load shapes, or advanced reporting, refer to the [Locust Documentation](https://docs.locust.io/en/stable/).
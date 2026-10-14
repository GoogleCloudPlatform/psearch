# PSearch API Load Testing

This directory contains Locust load tests for the PSearch Serving API as defined in the OpenAPI specification.

## Overview

The load testing setup simulates users performing various operations on the PSearch API:
- Health check requests
- Product search requests with various query parameters
- Tests with invalid parameters to verify error handling

## Setup

### Prerequisites

- Python 3.8+
- pip (Python package manager)

### Installation

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```bash
   # Copy the example env file and modify as needed
   cp .env.example .env
   
   # Edit the .env file with your preferred settings
   ```

   Or set environment variables directly in your shell:
   ```bash
   export API_HOST=https://product-search-api-1234567890.us-central1.run.app
   export MIN_WAIT_TIME=1000
   export MAX_WAIT_TIME=5000
   ```

## Running the Load Tests

### Starting Locust

Run Locust with:
```bash
# From the loadtest directory:
locust -f locustfile.py

# Or from the project root:
locust -f src/loadtest/locustfile.py
```

Override the host from command line (takes precedence over env vars):
```bash
locust -f locustfile.py --host=http://localhost:8080
```

### Web UI

Once Locust is running, open the web interface at http://localhost:8089 to:
1. Configure the number of users to simulate
2. Set the spawn rate (users per second)
3. Start the test
4. Monitor real-time results including response times, failure rates, and statistics

### Headless Mode

For CI/CD pipelines or automated testing, run Locust in headless mode:
```bash
locust -f locustfile.py --headless -u 100 -r 10 --run-time 5m --host=https://product-search-api-1234567890.us-central1.run.app
```

Where:
- `-u 100`: Simulates 100 users
- `-r 10`: Spawns 10 users per second
- `--run-time 5m`: Runs the test for 5 minutes

## Customizing Tests

### Adding New Search Queries

To add new search queries, edit the `search_queries.py` file:
```python
SEARCH_QUERIES.append({"query": "new product type", "limit": 15})
```

### Modifying User Behavior

To change how the simulated users behave, edit the task weights in `locustfile.py`. Higher weights mean the task is executed more frequently:
```python
@task(5)  # 5 times more frequent than tasks with weight 1
def search_products(self):
    # ...
```

## Output and Reporting

Locust provides several reporting options:

- CSV reports: `--csv=results`
- HTML reports: `--html=report.html`

Example:
```bash
locust -f locustfile.py --csv=results --html=report.html --headless -u 100 -r 10 --run-time 5m
```

This will generate:
- results_stats.csv (summary statistics)
- results_failures.csv (details of failures)
- results_exceptions.csv (Python exceptions during the test)
- report.html (HTML report)

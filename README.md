Kafka end-to-end perf test
==========================

TODO


How it works
------------

[What the test does](docs/imgs/diagram.png)

Note: `zmqrpc.py` was taken from https://github.com/locustio/locust as it provides handy abstraction. Thank you Locust team!


Installation
------------

Install with:

    python -m venv venv
    source venv/bin/activate
    pip install git+https://github.com/jhutar/kafka-e2e-perf-test.git

If you have cloned the git and want to develop locally, replace last step with:

    pip install --editable .


Usage
-----

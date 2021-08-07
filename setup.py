#!/usr/bin/env python3

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kafka-e2e-perf-test",
    version="0.0.1",
    maintainer="Jan Hutar",
    maintainer_email="jhutar@redhat.com",
    description="Kafka end-to-end perf test",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jhutar/kafka-e2e-perf-test",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: POSIX :: Linux",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Quality Assurance",
    ],
    python_requires='>=3.6',
    install_requires=[
        "kafka-python",
        "python-snappy",
        "opl-rhcloud-perf-team @ git+https://github.com/redhat-performance/opl.git",
    ],
    packages=[
        "kafka_e2e_perf_test",
    ],
    package_data={
        "kafka_e2e_perf_test": [
        ],
    },
    entry_points={
        "console_scripts": [
            "kafka-e2e-perf-test.py=kafka_e2e_perf_test.e2e:main",
        ],
    },
)

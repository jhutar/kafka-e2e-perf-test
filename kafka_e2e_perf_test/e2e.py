#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import argparse
import copy
import datetime
import itertools
import logging
import multiprocessing
import random
import string
import sys
import time
import uuid

from kafka import KafkaConsumer
from kafka import KafkaProducer

import kafka_e2e_perf_test.zmqrpc

import opl.data
import opl.date


def my_fromisoformat(string):
    return opl.date.my_fromisoformat(string)


def my_isoformat(dateobj):
    """My limited version of datetime.datetime.isoformat() function."""
    if dateobj.tzinfo != datetime.timezone.utc:
        raise ValueError(f"I do not know how to handle timezone for {dateobj}")
    return dateobj.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')


def split_integer(num, parts):
    """
    Split number into given number of pars that are as equal as possible

    https://stackoverflow.com/questions/55465884/how-to-divide-an-unknown-integer-into-a-given-number-of-even-parts-using-python
    """
    quotient, remainder = divmod(num, parts)
    lower_elements = [quotient for i in range(parts - remainder)]
    higher_elements = [quotient + 1 for j in range(remainder)]
    return lower_elements + higher_elements


def do_producer_process(args, store_here_file, start_producing_barrier, start_producing_event):
    def handle_send_success(record, metadata):
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        store_here_pointer.write(f"SUCCESS {metadata['uuid']} {my_isoformat(metadata['sent'])} {my_isoformat(now)}\n")

    def handle_send_error(excp, metadata):
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        store_here_pointer.write(f"ERROR {metadata['uuid']} {my_isoformat(metadata['sent'])} {my_isoformat(now)} {excp}\n")

    logger = logging.getLogger('script-e2e.do_producer_process')

    kafka_args = {
        'bootstrap_servers': f'{args.kafka_bootstrap_host}:{args.kafka_bootstrap_port}',
        'security_protocol': args.kafka_security_protocol,
        'acks': args.producer_acks,
        'compression_type': args.producer_compression_type,
        'batch_size': args.producer_batch_size,
        'linger_ms': args.producer_linger_ms,
        'buffer_memory': args.producer_buffer_memory,
        'max_block_ms': args.producer_max_block_ms,
        'max_request_size': args.producer_max_request_size,
        'request_timeout_ms': args.producer_request_timeout_ms,
        'send_buffer_bytes': args.producer_send_buffer_bytes,
        'max_in_flight_requests_per_connection': args.producer_max_in_flight_requests_per_connection,
    }
    if args.kafka_security_protocol == 'SASL_SSL':
        kafka_args['sasl_plain_username'] = args.kafka_sasl_plain_username
        kafka_args['sasl_plain_password'] = args.kafka_sasl_plain_password
        kafka_args['sasl_mechanism'] = args.kafka_sasl_mechanism

    producer = KafkaProducer(**kafka_args)

    logger.info(f"Generating {args.test_produce_messages} payloads")
    payload_random_part = ':' + ''.join(random.choices(string.ascii_lowercase, k=658))
    payloads = {}
    for _ in range(args.test_produce_messages):
        payload_uuid = str(uuid.uuid4())
        payload = ('UUID:' + payload_uuid + payload_random_part).encode('utf-8')
        payloads[payload_uuid] = payload

    logger.info(f"Opening logging file {store_here_file}")
    store_here_pointer = open(store_here_file, 'w')

    logger.info("Signaling I'm ready to produce")
    start_producing_barrier.wait()

    logger.info("Waiting for order to produce")
    start_producing_event.wait()

    logger.info(f"Producing generated payloads with rate {args.test_produce_rate} messages/sec")
    rate_current_sec = int(time.perf_counter())
    rate_counter = 0
    for payload_uuid, payload in payloads.items():
        future = producer.send(
            args.kafka_topic,
            value=payload,
            key=None,
            headers=None,
        )
        future_metadata = {
            'uuid': payload_uuid,
            'sent': datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc),
        }
        future.add_callback(handle_send_success, metadata=future_metadata)
        future.add_errback(handle_send_error, metadata=future_metadata)

        # If rate limitting is suposed to be active, do the counting
        if args.test_produce_rate != 0:
            rate_counter += 1
            if rate_counter == args.test_produce_rate:
                logger.debug(f"Rate limitting triggered at second {rate_current_sec} after sending {rate_counter} messages")
                while int(time.perf_counter()) == rate_current_sec:
                    time.sleep(0.01)
                rate_current_sec = int(time.perf_counter())
                rate_counter = 0
            elif int(time.perf_counter()) != rate_current_sec:
                logger.debug(f"Rate limitting lagging at second {rate_current_sec} after sending {rate_counter} messages")
                rate_current_sec = int(time.perf_counter())
                rate_counter = 0

    logger.info("Waiting for all messages to be published")
    # flush() and close() does not work for me, but this while loop
    # seems to be helping
    while producer._accumulator.has_unsent():
        logger.debug("There are still messages to be sent, waiting")
        time.sleep(0.1)

    logger.info("Quitting producer")
    producer.flush(timeout=10)
    producer.close(timeout=10)

    logger.info(f"Closing logging file {store_here_pointer.name}")
    store_here_pointer.close()


def do_consumer_process(args, store_here_file, start_consuming_barrier):
    logger = logging.getLogger('script-e2e.do_consumer_process')

    kafka_args = {
        'bootstrap_servers': f'{args.kafka_bootstrap_host}:{args.kafka_bootstrap_port}',
        'security_protocol': args.kafka_security_protocol,
        'group_id': args.consumer_group_id,
        'fetch_min_bytes': args.consumer_fetch_min_bytes,
        'fetch_max_wait_ms': args.consumer_fetch_max_wait_ms,
        'fetch_max_bytes': args.consumer_fetch_max_bytes,
        'max_partition_fetch_bytes': args.consumer_max_partition_fetch_bytes,
        'max_in_flight_requests_per_connection': args.consumer_max_in_flight_requests_per_connection,
        'enable_auto_commit': args.consumer_enable_auto_commit,
        'auto_commit_interval_ms': args.consumer_auto_commit_interval_ms,
        'check_crcs': args.consumer_check_crcs,
        'max_poll_records': args.consumer_max_poll_records,
        'max_poll_interval_ms': args.consumer_max_poll_interval_ms,
        'heartbeat_interval_ms': args.consumer_heartbeat_interval_ms,
        'receive_buffer_bytes': args.consumer_receive_buffer_bytes,
        'send_buffer_bytes': args.consumer_send_buffer_bytes,
        'consumer_timeout_ms': args.consumer_consumer_timeout_ms,
    }
    if args.kafka_security_protocol == 'SASL_SSL':
        kafka_args['sasl_plain_username'] = args.kafka_sasl_plain_username
        kafka_args['sasl_plain_password'] = args.kafka_sasl_plain_password
        kafka_args['sasl_mechanism'] = args.kafka_sasl_mechanism

    consumer = KafkaConsumer(args.kafka_topic, **kafka_args)

    logger.info(f"Opening logging file {store_here_file}")
    store_here_pointer = open(store_here_file, 'w')

    logger.info("Signaling I'm ready to consume")
    start_consuming_barrier.wait()

    logger.info("Starting consumer loop")
    counter = 0
    for message in consumer:
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        value = message.value.decode()
        if value.startswith('UUID:'):
            consumed = datetime.datetime.utcfromtimestamp(float(message.timestamp) / 1000).replace(tzinfo=datetime.timezone.utc)
            uuid = value.split(':')[1]
            store_here_pointer.write(f"RECEIVED {uuid} {my_isoformat(consumed)} {my_isoformat(now)} topic={message.topic} partition={message.partition} offset={message.offset}\n")
            counter += 1
        else:
            store_here_pointer.write(f"WARNING Unknown message {value}\n")

    logger.info(f"Closing logging file {store_here_pointer.name} after writing {counter} records")
    store_here_pointer.close()


def do_results(args):
    messages = {}

    logger = logging.getLogger('script-e2e.do_results')

    producer_failed_counter = 0
    with open(args.results_producer_log, 'r') as fp:
        for row in fp:
            row_list = row.strip().split(' ')
            if row_list[0] != 'SUCCESS':
                producer_failed_counter += 1
                continue

            messages[row_list[1]] = {
                'sent': my_fromisoformat(row_list[2]),   # when message was sent in our code
                'published': my_fromisoformat(row_list[3]),   # when on successful publish callback was called
            }

    consumer_failed_counter = 0
    with open(args.results_consumer_log, 'r') as fp:
        for row in fp:
            row_list = row.strip().split(' ')
            if row_list[0] != 'RECEIVED':
                consumer_failed_counter += 1
                continue

            if row_list[1] not in messages:
                messages[row_list[1]] = {}

            messages[row_list[1]].update({
                'output': my_fromisoformat(row_list[2]),   # time stamp reported by the message
                'consumed': my_fromisoformat(row_list[3]),   # when our consumer noticed it
            })

    total_messages_count = len(messages)
    messages = [v for v in messages.values() if 'sent' in v and 'published' in v and 'output' in v and 'consumed' in v]
    complete_messages_count = len(messages)

    for m in messages:
        m['consumed-sent'] = (m['consumed'] - m['sent']).total_seconds()
        m['consumed-published'] = (m['consumed'] - m['published']).total_seconds()

    print(f'Producer failures: {producer_failed_counter}')
    print(f'Consumer failures: {consumer_failed_counter}')
    print(f'Total messages: {total_messages_count}')
    print(f'Common messages: {complete_messages_count}')

    def _show(data, metric):
        print(f"{metric} stats:")
        stats = opl.data.data_stats([i[metric] for i in messages])
        for k, v in stats.items():
            if isinstance(v, datetime.datetime):
                v = my_isoformat(v)
            print(f"    {k}: {v}")
        return stats

    stats = _show(messages, 'sent')
    if stats['samples'] == 0:
        logger.fatal("No data measured!")
        sys.exit(1)

    stats_sent_range = stats['range'].total_seconds()
    stats = _show(messages, 'published')
    stats_published_range = stats['range'].total_seconds()
    stats = _show(messages, 'output')
    stats_output_range = stats['range'].total_seconds()
    stats = _show(messages, 'consumed')
    stats_consumed_range = stats['range'].total_seconds()
    stats = _show(messages, 'consumed-sent')
    stats_consumed_sent_mean = stats['mean']
    stats_consumed_sent_25th = stats['percentile25']
    stats_consumed_sent_50th = stats['median']
    stats_consumed_sent_75th = stats['percentile75']
    stats_consumed_sent_90th = stats['percentile90']
    stats_consumed_sent_99th = stats['percentile99']
    stats_consumed_sent_999th = stats['percentile999']
    stats_consumed_sent_max = stats['max']
    stats = _show(messages, 'consumed-published')

    print(f"{complete_messages_count} {stats_sent_range} {stats_published_range} {stats_output_range} {stats_consumed_range} {stats_consumed_sent_mean} {stats_consumed_sent_25th} {stats_consumed_sent_50th} {stats_consumed_sent_75th} {stats_consumed_sent_90th} {stats_consumed_sent_99th} {stats_consumed_sent_999th} {stats_consumed_sent_max}")


def do_standalone(args):
    logger = logging.getLogger("script-e2e.do_standalone")

    consumer_args = []
    producer_args = []

    test_produce_messages_per_producer = split_integer(args.test_produce_messages, args.test_producer_processes)
    test_produce_rate_per_producer = split_integer(args.test_produce_rate, args.test_producer_processes)

    # Prepare arguments for consumer and producer processes
    for i in range(args.test_consumer_processes):
        args_this = copy.deepcopy(args)
        consumer_args.append((args_this, args.results_consumer_log + "." + str(len(consumer_args))))

    for i in range(args.test_producer_processes):
        args_this = copy.deepcopy(args)
        args_this.test_produce_messages = test_produce_messages_per_producer[i]
        args_this.test_produce_rate = test_produce_rate_per_producer[i]
        producer_args.append((args_this, args.results_producer_log + "." + str(len(producer_args))))

    # This barrier serves for block main thread untill all producer
    # processes finished initialization. The "+1" is there for a main
    # (this one) process.
    start_producing_barrier = multiprocessing.Barrier(len(producer_args) + 1, timeout=100)

    # This event serves for letting producer processes to know they
    # should start producing
    start_producing_event = multiprocessing.Event()

    # Start producer processes
    logger.info(f"Starting {len(producer_args)} producer processes")
    producer_processes = []
    for process_args in producer_args:
        p = multiprocessing.Process(target=do_producer_process, args=process_args + (start_producing_barrier, start_producing_event))
        p.start()
        producer_processes.append(p)

    # Block untill all producers are ready
    logger.info("Waiting for producer processes to initialize")
    start_producing_barrier.wait()

    # This barrier serves for block main thread untill all consumers
    # processes finished initialization. The "+1" is there for a main
    # (this one) process.
    start_consuming_barrier = multiprocessing.Barrier(len(consumer_args) + 1, timeout=100)

    # Start consumer processes
    logger.info(f"Starting {len(consumer_args)} consumer processes")
    consumer_processes = []
    for process_args in consumer_args:
        p = multiprocessing.Process(target=do_consumer_process, args=process_args + (start_consuming_barrier,))
        p.start()
        consumer_processes.append(p)

    # Block untill all consumers are ready
    logger.info("Waiting for consumer processes to initialize")
    start_consuming_barrier.wait()

    # Start producing messages
    logger.info("Signaling producer processes to start producing")
    start_producing_event.set()

    # Wait for producers to finish
    logger.info("Waiting for producer processes to finish")
    for p in producer_processes:
        p.join()
        logger.info(f"Producer process {p.pid} exited with {p.exitcode}")

    # Wait for consumers to finish
    logger.info("Waiting for consumer processes to finish")
    for p in consumer_processes:
        p.join()
        logger.info(f"Consumer process {p.pid} exited with {p.exitcode}")

    # Merge consumer data
    with open(args.results_consumer_log, 'w') as fp_w:
        for i in range(len(consumer_args)):
            f = args.results_consumer_log + "." + str(i)
            logger.info(f"Merging consumer data from {f}")
            with open(f, 'r') as fp_r:
                for row in fp_r:
                    fp_w.write(row)

    # Merge producer data
    with open(args.results_producer_log, 'w') as fp_w:
        for i in range(len(producer_args)):
            f = args.results_producer_log + "." + str(i)
            logger.info(f"Merging producer data from {f}")
            with open(f, 'r') as fp_r:
                for row in fp_r:
                    fp_w.write(row)


def do_leader(args):
    logger = logging.getLogger("script-e2e.do_leader")

    connection = kafka_e2e_perf_test.zmqrpc.Server("*", args.leader_port)

    test_produce_messages_per_producer = split_integer(args.test_produce_messages, args.test_producer_processes)
    test_produce_rate_per_producer = split_integer(args.test_produce_rate, args.test_producer_processes)

    required_capacity = args.test_producer_processes + args.test_consumer_processes
    followers = {}

    # Wait for followers to offer their capacity (OFFERING_CAPACITY)
    while True:
        follower_id, follower_msg = connection.recv_from_client()
        if follower_msg.type != 'OFFERING_CAPACITY':
            logger.debug(f"Ignoring this {follower_msg.type} message")
            continue

        logger.debug(f"Follower {follower_id} message {follower_msg.type}: {follower_msg.data}")

        followers[follower_id] = {
            "available_capacity": follower_msg.data["capacity"],
            "consumers_started": 0,
            "producers_started": 0,
            "consumers_initiated": 0,
            "producers_initiated": 0,
            "consumers_finished": 0,
            "producers_finished": 0,
            "consumers_data_transfered": 0,
            "producers_data_transfered": 0,
        }

        if len(followers) == args.leader_expect_offers:
            logger.info(f"All {args.leader_expect_offers} followers offered their capacity: {', '.join(followers.keys())}")
            break

    available_capacity = sum([i["available_capacity"] for i in followers.values()])
    assert available_capacity >= required_capacity, f"{args.leader_expect_offers} followers provided capacity {available_capacity} but we require {required_capacity}. Try with beefier followers please."

    # Prepare args we will hand to followers so they can configure producers/consumers
    args_ = {k: v for k, v in vars(args).items() if k.startswith('kafka_') or k.startswith('producer_') or k.startswith('consumer_')}

    # Prepare arguments for processes (ALLOCATE_CAPACITY and then DONE_ALLOCATING_CAPACITY)
    for follower_id in itertools.cycle(followers.keys()):
        consumers_started = sum([i["consumers_started"] for i in followers.values()])
        if consumers_started < args.test_consumer_processes:
            logger.debug(f"Asking follower {follower_id} to start consumer")
            connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("ALLOCATE_CAPACITY", {"workload": "consumer", "args": args_}, follower_id))
            followers[follower_id]["consumers_started"] += 1

        producers_started = sum([i["producers_started"] for i in followers.values()])
        if producers_started < args.test_producer_processes:
            logger.debug(f"Asking follower {follower_id} to start producer")
            args_['test_produce_messages'] = test_produce_messages_per_producer[producers_started]
            args_['test_produce_rate'] = test_produce_rate_per_producer[producers_started]
            connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("ALLOCATE_CAPACITY", {"workload": "producer", "args": args_}, follower_id))
            followers[follower_id]["producers_started"] += 1

        if consumers_started >= args.test_consumer_processes and producers_started >= args.test_producer_processes:
            logger.info(f"Started {consumers_started} consumers and {producers_started} producers")
            for follower_id in followers.keys():
                connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("DONE_ALLOCATING_CAPACITY", None, follower_id))
            break

    # Now make followers to start producers (START_PRODUCERS)
    for follower_id in followers.keys():
        if followers[follower_id]["producers_started"] > 0:
            connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("START_PRODUCERS", None, follower_id))

    # Wait for producers being initiated (PRODUCERS_INITIATED)
    while True:
        follower_id, follower_msg = connection.recv_from_client()
        logger.debug(f"Follower {follower_id} message {follower_msg.type}: {follower_msg.data}")
        if follower_msg.type != "PRODUCERS_INITIATED":
            logger.debug(f"Ignoring this {follower_msg.type} message")
            continue

        followers[follower_id]["producers_initiated"] += follower_msg.data['initiated']

        producers_initiated = sum([i["producers_initiated"] for i in followers.values()])
        if producers_initiated >= args.test_producer_processes:
            logger.info(f"Initiated {producers_initiated} producers")
            break

    # Trigger consumers start (START_CONSUMERS)
    for follower_id in followers.keys():
        if followers[follower_id]["consumers_started"] > 0:
            connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("START_CONSUMERS", None, follower_id))

    # Wait for consumers being initiated (CONSUMERS_INITIATED)
    while True:
        follower_id, follower_msg = connection.recv_from_client()
        logger.debug(f"Follower {follower_id} message {follower_msg.type}: {follower_msg.data}")
        followers[follower_id]["consumers_initiated"] += follower_msg.data['initiated']

        consumers_initiated = sum([i["consumers_initiated"] for i in followers.values()])
        if consumers_initiated >= args.test_consumer_processes:
            logger.info(f"Initiated {consumers_initiated} consumers")
            break

    # Trigger producing messages (START_PRODUCING)
    for follower_id in followers.keys():
        if followers[follower_id]["producers_started"] > 0:
            logger.debug(f"Triggering message producing on {follower_id}")
            connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("START_PRODUCING", None, follower_id))

    # Wait for producers and consumers to finish (CONSUMER_FINISHED and PRODUCER_FINISHED)
    while True:
        follower_id, follower_msg = connection.recv_from_client()
        logger.debug(f"Follower {follower_id} message {follower_msg.type}: {follower_msg.data}")
        if follower_msg.type not in ("CONSUMER_FINISHED", "PRODUCER_FINISHED"):
            logger.debug(f"Ignoring this {follower_msg.type} message")
            continue
        assert follower_msg.data["exitcode"] == 0

        if follower_msg.type == "CONSUMER_FINISHED":
            followers[follower_id]["consumers_finished"] += 1
        if follower_msg.type == "PRODUCER_FINISHED":
            followers[follower_id]["producers_finished"] += 1

        consumers_finished = sum([i["consumers_finished"] for i in followers.values()])
        producers_finished = sum([i["producers_finished"] for i in followers.values()])

        if consumers_finished >= args.test_consumer_processes and producers_finished >= args.test_producer_processes:
            logger.info(f"All {consumers_finished} consumers and {producers_finished} producers finished")
            break

    # Ask for data (SEND_DATA)
    for follower_id in followers.keys():
        connection.send_to_client(kafka_e2e_perf_test.zmqrpc.Message("SEND_DATA", None, follower_id))

    # Wait for all the data
    consumer_counter = 0
    producer_counter = 0
    consumer_log_fd = open(args.results_consumer_log, 'w')
    producer_log_fd = open(args.results_producer_log, 'w')
    while True:
        follower_id, follower_msg = connection.recv_from_client()
        logger.debug(f"Follower {follower_id} message {follower_msg.type}: {str(follower_msg.data)[:100]}")
        if follower_msg.type not in ("CONSUMER_DATA", "PRODUCER_DATA"):
            logger.debug(f"Ignoring this {follower_msg.type} message")
            continue

        if follower_msg.type == "CONSUMER_DATA":
            for row in follower_msg.data["data"]:
                consumer_log_fd.write(row + "\n")
            consumer_counter += len(follower_msg.data["data"])
            if follower_msg.data["done"]:
                logger.debug("This was last batch for consumer")
                followers[follower_id]["consumers_data_transfered"] += 1
        if follower_msg.type == "PRODUCER_DATA":
            for row in follower_msg.data["data"]:
                producer_log_fd.write(row + "\n")
            producer_counter += len(follower_msg.data["data"])
            if follower_msg.data["done"]:
                logger.debug("This was last batch for producer")
                followers[follower_id]["producers_data_transfered"] += 1

        consumers_data_transfered = sum([i["consumers_data_transfered"] for i in followers.values()])
        producers_data_transfered = sum([i["producers_data_transfered"] for i in followers.values()])

        if consumers_data_transfered >= args.test_consumer_processes and producers_data_transfered >= args.test_producer_processes:
            logger.info(f"All {consumers_data_transfered} consumers ({consumer_counter} lines) and {producers_data_transfered} producers ({producer_counter} lines) sent it's data")
            break
    consumer_log_fd.close()
    producer_log_fd.close()


def do_follower(args):
    follower_id = str(uuid.uuid4())

    logger = logging.getLogger(f"script-e2e.do_follower({follower_id})")

    connection = kafka_e2e_perf_test.zmqrpc.Client(args.leader_host, args.leader_port, follower_id)

    if args.follower_offer_capacity == 0:
        args.follower_offer_capacity = multiprocessing.cpu_count()
    logger.debug(f"Offering capacity: {args.follower_offer_capacity}")
    connection.send(kafka_e2e_perf_test.zmqrpc.Message("OFFERING_CAPACITY", {"capacity": args.follower_offer_capacity}, follower_id))

    consumer_args = []
    producer_args = []

    # Prepare arguments for consumer and producer processes
    while True:
        msg = connection.recv()
        logger.debug(f"Received: {msg.type} {msg.data}")

        if msg.type == 'DONE_ALLOCATING_CAPACITY':
            break

        assert msg.type == 'ALLOCATE_CAPACITY'
        assert msg.data['workload'] in ('consumer', 'producer')

        args_this = copy.deepcopy(args)
        for k, v in msg.data['args'].items():
            setattr(args_this, k, v)

        if msg.data['workload'] == 'consumer':
            consumer_args.append((args_this, args.results_consumer_log + "." + str(len(consumer_args))))
        if msg.data['workload'] == 'producer':
            producer_args.append((args_this, args.results_producer_log + "." + str(len(producer_args))))

    # Wait for signal to start producers
    while True:
        msg = connection.recv()
        logger.debug(f"Received: {msg.type} {msg.data}")
        if msg.type == "START_PRODUCERS":
            break

    # This barrier serves for block main thread untill all producer
    # processes finished initialization. The "+1" is there for a main
    # (this one) process.
    start_producing_barrier = multiprocessing.Barrier(len(producer_args) + 1, timeout=100)

    # This event serves for letting producer processes to know they
    # should start producing
    start_producing_event = multiprocessing.Event()

    # Start producer processes
    logger.info(f"Starting {len(producer_args)} producer processes")
    producer_processes = []
    for process_args in producer_args:
        p = multiprocessing.Process(target=do_producer_process, args=process_args + (start_producing_barrier, start_producing_event))
        p.start()
        producer_processes.append(p)

    # Block untill all producers are ready
    logger.info("Waiting for producer processes to initialize")
    start_producing_barrier.wait()
    connection.send(kafka_e2e_perf_test.zmqrpc.Message("PRODUCERS_INITIATED", {"initiated": len(producer_processes)}, follower_id))

    # This barrier serves for block main thread untill all consumers
    # processes finished initialization. The "+1" is there for a main
    # (this one) process.
    start_consuming_barrier = multiprocessing.Barrier(len(consumer_args) + 1, timeout=100)

    # Wait for signal to start consumers
    while True:
        msg = connection.recv()
        logger.debug(f"Received: {msg.type} {msg.data}")
        if msg.type == "START_CONSUMERS":
            break

    # Start consumer processes
    logger.info(f"Starting {len(consumer_args)} consumer processes")
    consumer_processes = []
    for process_args in consumer_args:
        p = multiprocessing.Process(target=do_consumer_process, args=process_args + (start_consuming_barrier,))
        p.start()
        consumer_processes.append(p)

    # Block untill all consumers are ready
    logger.info("Waiting for consumer processes to initialize")
    start_consuming_barrier.wait()
    connection.send(kafka_e2e_perf_test.zmqrpc.Message("CONSUMERS_INITIATED", {"initiated": len(consumer_processes)}, follower_id))

    # Wait for signal to start producing messages
    while True:
        msg = connection.recv()
        logger.debug(f"Received: {msg.type} {msg.data}")
        if msg.type == "START_PRODUCING":
            break

    # Start producing messages
    logger.info("Signaling producer processes to start producing")
    start_producing_event.set()

    # Wait for producers to finish
    logger.info("Waiting for producer processes to finish")
    for p in producer_processes:
        p.join()
        logger.info(f"Producer process {p.pid} exited with {p.exitcode}")
        connection.send(kafka_e2e_perf_test.zmqrpc.Message("PRODUCER_FINISHED", {"pid": p.pid, "exitcode": p.exitcode}, follower_id))

    # Wait for consumers to finish
    logger.info("Waiting for consumer processes to finish")
    for p in consumer_processes:
        p.join()
        logger.info(f"Consumer process {p.pid} exited with {p.exitcode}")
        connection.send(kafka_e2e_perf_test.zmqrpc.Message("CONSUMER_FINISHED", {"pid": p.pid, "exitcode": p.exitcode}, follower_id))

    # Wait for order to send data
    while True:
        msg = connection.recv()
        logger.debug(f"Received: {msg.type} {msg.data}")
        if msg.type == "SEND_DATA":
            break

    # Send consumer data
    for i in range(len(consumer_args)):
        f = args.results_consumer_log + "." + str(i)
        logger.info(f"Sending consumer data from {f}")
        with open(f, 'r') as fp:
            data = []
            for row in fp:
                data.append(row.strip())
                if len(data) > 1000:
                    connection.send(kafka_e2e_perf_test.zmqrpc.Message("CONSUMER_DATA", {"done": False, "data": data}, follower_id))
                    data = []
            connection.send(kafka_e2e_perf_test.zmqrpc.Message("CONSUMER_DATA", {"done": True, "data": data}, follower_id))

    # Send producer data
    for i in range(len(producer_args)):
        f = args.results_producer_log + "." + str(i)
        logger.info(f"Sending producer data from {f}")
        with open(f, 'r') as fp:
            data = []
            for row in fp:
                data.append(row.strip())
                if len(data) > 1000:
                    connection.send(kafka_e2e_perf_test.zmqrpc.Message("PRODUCER_DATA", {"done": False, "data": data}, follower_id))
                    data = []
            connection.send(kafka_e2e_perf_test.zmqrpc.Message("PRODUCER_DATA", {"done": True, "data": data}, follower_id))


def main():
    parser = argparse.ArgumentParser(description='Helper tool for Kafka e2e latency test')
    parser.add_argument('action', choices=['standalone', 'leader', 'follower', 'results'],
                        help='What shall we do?')
    parser.add_argument('--leader-host', default='localhost',
                        help='Where is our leader running? When started as a leader, we listen on *.')
    parser.add_argument('--leader-port', default=20000, type=int,
                        help='What is the leader port?')
    parser.add_argument('--leader-expect-offers', default=1, type=int,
                        help='How many offers should we expect (how many folowers)?')
    parser.add_argument('--follower-offer-capacity', default=0, type=int,
                        help='How many producers/consumers can this follower handle? Defaults to number of CPUs.')
    parser.add_argument('--kafka-bootstrap-host', default='ec2-18-117-240-4.us-east-2.compute.amazonaws.com',
                        help='What Kafka bootstrap server to connect to?')
    parser.add_argument('--kafka-bootstrap-port', default='9092',
                        help='What Kafka bootstrap server port to connect to?')
    parser.add_argument('--kafka-security-protocol', default='PLAINTEXT',
                        choices=['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'],
                        help='Protocol used to communicate with brokers.')
    parser.add_argument('--kafka-sasl-mechanism',
                        choices=['PLAIN', 'GSSAPI', 'OAUTHBEARER', 'SCRAM-SHA-256', 'SCRAM-SHA-512'],
                        help='Authentication mechanism when security_protocol is configured for SASL_PLAINTEXT or SASL_SSL.')
    parser.add_argument('--kafka-sasl-plain-username',
                        help='Username for sasl PLAIN and SCRAM mechamism.')
    parser.add_argument('--kafka-sasl-plain-password',
                        help='Password for sasl PLAIN and SCRAM mechamism.')
    parser.add_argument('--kafka-topic', default='jhutar-test',
                        help='What topic should we produce to and consume from?')
    parser.add_argument('--test-producer-processes', type=int, default=1,
                        help='How many consumer processes should we start?')
    parser.add_argument('--test-consumer-processes', type=int, default=1,
                        help='How many consumer processes should we start?')
    parser.add_argument('--test-produce-messages', type=int, default=100,
                        help='How many messages should we produce in total (all proceses together)?')
    parser.add_argument('--test-produce-rate', type=int, default=0,
                        help='How many meessages should we produce per second in total (all processes together, use 0 for max)')
    parser.add_argument('--producer-acks', choices=['0', '1', 'all'], default='1',
                        help='What acks setting should producer use?')
    parser.add_argument('--consumer-consumer-timeout-ms', type=int, default=15000,
                        help='How long to wait when no message is comming?')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Show debug output')
    args = parser.parse_args()

    logger = logging.getLogger('script-e2e')
    if args.debug:
        logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(processName)s %(threadName)s %(name)s %(levelname)s %(message)s')
    handler_stream = logging.StreamHandler()
    handler_stream.setFormatter(formatter)
    logger.addHandler(handler_stream)
    handler_file = logging.FileHandler('/tmp/script-e2e.log')
    handler_file.setLevel(logging.DEBUG)
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

    # Post process args that need post processing
    if args.producer_acks != 'all':
        args.producer_acks = int(args.producer_acks)

    # Sanity check params the need checking
    if args.test_produce_rate != 0:
        assert args.test_produce_rate >= args.test_producer_processes, f"Producer rate {args.test_produce_rate} too low. Needs to be at least 1 for each of {args.test_producer_processes} producers"

    # Some extra defaults for producer
    args.producer_compression_type = None
    args.producer_batch_size = 16 * 1024
    args.producer_linger_ms = 0
    args.producer_buffer_memory = 32 * 1024 * 1024
    args.producer_max_block_ms = 60000
    args.producer_max_request_size = 1024 * 1024
    args.producer_request_timeout_ms = 30000
    args.producer_send_buffer_bytes = None
    args.producer_max_in_flight_requests_per_connection = 5

    # Some extra defaults for consumer
    args.consumer_group_id = 'script-e2e-consumer-group-1'
    args.consumer_fetch_min_bytes = 1
    args.consumer_fetch_max_wait_ms = 500
    args.consumer_fetch_max_bytes = 50 * 1024 * 1024
    args.consumer_max_partition_fetch_bytes = 10 * 1024 * 1024
    args.consumer_max_in_flight_requests_per_connection = 5
    args.consumer_enable_auto_commit = True
    args.consumer_auto_commit_interval_ms = 5000
    args.consumer_check_crcs = True
    args.consumer_max_poll_records = 500
    args.consumer_max_poll_interval_ms = 300000
    args.consumer_heartbeat_interval_ms = 3000
    args.consumer_receive_buffer_bytes = None
    args.consumer_send_buffer_bytes = None

    # Some extra defaults for results formatter
    args.results_producer_log = '/tmp/producer.log'
    args.results_consumer_log = '/tmp/consumer.log'

    logger.debug(f"Args: {args}")

    if args.action == 'standalone':
        do_standalone(args)
    elif args.action == 'leader':
        do_leader(args)
    elif args.action == 'follower':
        do_follower(args)
    elif args.action == 'results':
        do_results(args)
    else:
        raise Exception('Unknown action')


if __name__ == '__main__':
    main()

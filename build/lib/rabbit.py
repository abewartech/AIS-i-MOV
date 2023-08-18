#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 12:15:15 2017

@author: rory
"""
import logging
import logging.handlers
import os
import time

from kombu import Connection, Exchange, Queue

log = logging.getLogger("main.lib.rabbit")


def errback(exc, interval):
    log.warning("Consumer error: %r", exc)
    log.warning("Retry in %s +1  seconds.", interval)
    time.sleep(float(interval) + 1)
    return


class Rabbit_Producer:
    def __init__(
        self,
        amqp_url,
        exchange_name,
        queue_name,
        connection_args={},
        exchange_args={"type": "topic"},
        queue_args={},
    ):
        connection_args.update({"hostname": amqp_url})
        exchange_args.update({"name": exchange_name})

        self.connection = Connection(**connection_args)
        self.exchange = Exchange(**exchange_args)
        queue_args.update(
            {
                "name": queue_name,
                "exchange": self.exchange,
                "channel": self.connection,
            }
        )

        self.queue = Queue(**queue_args)

    def send_message(self, message, routing_key):
        self.queue.routing_key = routing_key
        self.queue.declare()
        with self.connection.Producer() as producer:
            producer.publish(
                message,
                exchange=self.exchange,
                routing_key=routing_key,
                declare=[self.queue],
            )

    def produce(self, message, routing_key, errback_func=errback):
        producer = self.connection.ensure(
            self, self.send_message, errback=errback_func, interval_start=1.0
        )
        producer(message, routing_key=routing_key)


class DockerRabbitProducer(Rabbit_Producer):
    def __init__(self):
        log.info("Setting up RabbitMQ sink interface...")
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(
            os.getenv("MOV_RABBITMQ_DEFAULT_USER"),
            os.getenv("MOV_RABBITMQ_DEFAULT_PASS"),
            os.getenv("MOV_RABBIT_HOST"),
            os.getenv("MOV_RABBIT_MSG_PORT"),
        )
        log.info("Source/Sink Rabbit is at {0}".format(self.rabbit_url))
        self.exchange_name = os.getenv("AISIMOV_RABBIT_EXCHANGE")
        self.queue_name = os.getenv("AISIMOV_RABBIT_QUEUE")  # TODO: QUEUE NAME

        log.info("Producer init complete")

        super().__init__(self.rabbit_url, self.exchange_name, self.queue_name)

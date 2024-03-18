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

from rab_the_bit import RabbitProducer

log = logging.getLogger("main.lib.rabbit")


def errback(exc, interval):
    log.warning("Consumer error: %r", exc)
    log.warning("Retry in %s +1  seconds.", interval)
    time.sleep(float(interval) + 1)
    return


class DockerRabbitProducer(RabbitProducer):
    def __init__(self):
        log.info("Setting up RabbitMQ sink interface...")
        # Key to consume from:
        RABBITMQ_USER = os.getenv("MOV_RABBITMQ_DEFAULT_USER")
        RABBITMQ_PASS = os.getenv("MOV_RABBITMQ_DEFAULT_PASS")
        RABBIT_HOST = os.getenv("MOV_RABBIT_HOST")
        RABBIT_MSG_PORT = os.getenv("MOV_RABBIT_MSG_PORT")

        self.rabbit_url = (
            f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}"
            f"@{RABBIT_HOST}:{RABBIT_MSG_PORT}/"
        )

        log.info("Source/Sink Rabbit is at {0}".format(self.rabbit_url))
        self.exchange_name = os.getenv("AISIMOV_RABBIT_EXCHANGE")
        self.queue_name = os.getenv("AISIMOV_RABBIT_QUEUE")
        self.queue_args = {'max_length':int(os.getenv("QUEUE_MAX_LENGTH"))}

        log.info("Producer init complete")

        super().__init__(
            self.rabbit_url,
            self.exchange_name,
            self.queue_name,
            self.queue_args,
            log=log,
            errback=errback,
        )

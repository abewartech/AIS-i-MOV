#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 12:15:15 2017

@author: rory
""" 
import logging
import logging.handlers

from threading import Timer


log = logging.getLogger('main.funcs') 


class Watchdog(Exception):
    '''
    Watchdog timer implementation used to restart the container 
    when it fails to publish a new message within X seconds.

    Taken from
    https://stackoverflow.com/questions/16148735/how-to-implement-a-watchdog-timer-in-python?noredirect=1&lq=1
    ''' 
    def __init__(self, timeout, userHandler=None):  # timeout in seconds
        log.info(f'Setting up watchdog timer for {timeout} seconds.') 
        self.timeout = timeout
        self.handler = userHandler if userHandler is not None else self.defaultHandler
        self.timer = Timer(self.timeout, self.handler)
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.timer = Timer(self.timeout, self.handler)
        self.timer.start()

    def stop(self):
        self.timer.cancel()

    def defaultHandler(self):
        raise self
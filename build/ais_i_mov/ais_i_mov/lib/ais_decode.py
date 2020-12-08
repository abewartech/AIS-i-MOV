#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
"""
import bz2
#import pandas as pd
import time as timer
import logging
import os
import re
import time
import datetime

import ais

import lib.file_streamer
import lib.funcs 


log = logging.getLogger('main.ais_decode')
# log.setLevel('DEBUG')


class AIS_Decoder():
    def __init__():
        pass
    def eta_from_multi(self, decoded_dict, event_time):
        # Turn the ETA items in Voyage reports into a datetime object
        try:
            event_timestamp = datetime.datetime.fromtimestamp(int(event_time))
            eta_datetime = datetime.datetime(year=event_timestamp.year,
                                            month=decoded_dict['eta_month'],
                                            day=decoded_dict['eta_day'],
                                            hour=decoded_dict['eta_hour'],
                                            minute=decoded_dict['eta_minute']).isoformat()
        except Exception as err:
            #generally the eta's have 60 minutes or day 0 or something like that 
            log.debug(err)
            eta_datetime = None
        return eta_datetime

    def parse_decode(self, line):
        log.debug('Parsing: {0}'.format(line))
        parsed_line = {}
        parsed_line['rx_time'], parsed_line['meta'], parsed_line['ais'] = re.split(r": \\|\\", line)
        parsed_line['meta'], parsed_line['meta_checksum'] = re.split(r"\*",parsed_line['meta'])
        time_found  =  re.search(r'[0-9]{10}',parsed_line['meta'])
        if time_found:
            parsed_line['event_time'] = time_found.group(0)
        else:
            parsed_line['event_time'] = None 
        parsed_line['talker'], parsed_line['frag_count'],parsed_line['frag_num'],parsed_line['seq_id'],parsed_line['radio_chan'], parsed_line['payload'], parsed_line['padding'],parsed_line['checksum'] = re.split(r',|\*', parsed_line['ais'])
    
        log.debug('Parsed: {0}'.format(parsed_line))
        return parsed_line

    def single_decode(self, parsed_line):
        log.debug('SingleLine')
        decoded_line = ais_decode(parsed_line)

        #Clean up Trailing @'s
        if 'callsign' in decoded_line: 
            decoded_line['callsign'] = decoded_line['callsign'].rstrip("@")
        if 'name' in decoded_line:
            decoded_line['name'] = decoded_line['name'].rstrip("@")
        if 'destination' in decoded_line:
            decoded_line['destination'] = decoded_line['destination'].rstrip("@")

        return decoded_line

    def multi_decode(self, parsed_line, next_line):
        log.debug('Multiline')
        parsed_line['payload'] = parsed_line.get('payload') + next_line.get('payload')
        parsed_line['padding'] = next_line['padding']  
        decoded_line = ais_decode(parsed_line) 

        #Clean up Trailing @'s
        if 'callsign' in decoded_line: 
            decoded_line['callsign'] = decoded_line['callsign'].rstrip("@ ")
        if 'name' in decoded_line:
            decoded_line['name'] = decoded_line['name'].rstrip("@ ")
        if 'destination' in decoded_line:
            decoded_line['destination'] = decoded_line['destination'].rstrip("@ ")

        # Turn eta Columns into datetime. 
        if 'eta_month' in decoded_line:
            decoded_line['eta'] = eta_from_multi(decoded_line, parsed_line['event_time'])

        log.debug('Line Decoded: {0}'.format(decoded_line))
        return decoded_line

    def ais_decode(self, parsed_line):
        log.debug('Decoding: {0}'.format(parsed_line))
        log.debug('Decoding: {0}, {1}'.format(parsed_line['payload'], int(parsed_line['padding'])))
        try:
            decode_dict = ais.decode(parsed_line['payload'], int(parsed_line['padding']))
        except Exception as err:
            log.debug(err)
            try:
                padding = 71 - len(parsed_line['payload'])
                decode_dict = ais.decode(parsed_line['payload'], padding) 
            except Exception as err:   
                decode_dict = {}
                log.debug(err)
        log.debug('Decoding: {0}'.format(decode_dict))
        return decode_dict

    def decode(self, msg):
        '''
        Take the message, parse it and decode it. return a decoded dict.
        '''
        log.info('Parsing MSG: '+ str(msg))
        try:
            parsed_line = parse_decode(line)
            if parsed_line.get('frag_count') == '1':
                decoded_line = single_decode(parsed_line)
            else:
                next_line = open_file.readline()
                parsed_next_line = parse_decode(next_line)
                decoded_line = multi_decode(parsed_line, parsed_next_line)

            decoded_line['routing_key'] = os.getenv('routing_key')
            decoded_line['event_time'] = datetime.datetime.fromtimestamp(int(parsed_line.get('event_time'))).isoformat()
            log.debug(decoded_line)
        except:
            log.warning('Problem with parsing and decoding line: {0}'.format(line))
            

        try:                         
            if decoded_line.get('id') in [1,2,3,5,9,18,19]:
                log.debug('Rabbit_Produce')
                rabbit.produce_msg(decoded_line) 
        except TimeoutError as Terr:
            # Something went wrong with the connection, rebuild it.
            log.warning('Timeout ERROR: {0}'.format(Terr))
            rabbit = lib.funcs.Rabbit_Wrapper(CFG) 
        except Exception as err:
            log.warning(err)
            pass
        end = time.time()
        log.info('Did {0} messages in {1} seconds.'.format(line_count, (end-start)))
        log.info('Sleeping for %s seconds.',CFG.get('file_sleep'))
        timer.sleep(int(CFG.get('file_sleep')))
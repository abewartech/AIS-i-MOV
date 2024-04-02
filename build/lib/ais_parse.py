#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
"""
import datetime
import logging
import os
import re
from typing import List

from lib.ais_atributes_standards import talker_ids

log = logging.getLogger("ais_parse")

TIMESTAMP_DIVISOR = int(os.getenv('TIMESTAMP_DIVISOR',default = 1))  # because the timestamp is in milliseconds
# TIMESTAMP_DIVISOR = 1000  # because the timestamp is in milliseconds
MIN_LINE_LENGTH = 10


class AIS_Line:
    def __init__(self, line: str):
        self.parse_line(line)
        # self._set_standards_atributes()

    def parse_line(self, line):
        line = line.replace("\n", "")
        try:
            header, self.ais_data = [f for f in line.split("\\") if f]
        except:
            self.ais_data = line
            header = ""
        if header:
            self.header_dict = {
                item.split(":", 1)[0]: item.split(":", 1)[1]
                for item in header.split(",")
            }
        else:
            self.header_dict = {}
        # !AIVDM,1,1,,B,33ku82U000OGsfHH:Uv`9j3J0>@<,0*68
        # TODO: check names for meta and meta_2
        (
            self.meta,
            self.num_lines,
            self.num_this_line,
            self.group_id,
            self.meta_2,
            self.ais_msg,
            self.checksum,
        ) = self.ais_data.split(",")
        self.num_lines = int(self.num_lines)
        self.num_this_line = int(self.num_this_line)
        self.multiline = self.num_lines > 1

    def _set_standards_atributes(self):
        # TODO: check if needed!?
        """
        Based on https://gpsd.gitlab.io/gpsd/AIVDM.html
        """
        # Talker IDS
        talker_id = self.meta
        self.talker_id = talker_id.replace("!", "")[:2]
        self.talker_id_description = talker_ids[self.talker_id]

        # AIS Payload Interpretation


class AIS_Message:
    def __init__(self):
        self._reset()

    def _reset(self):
        self.head_dict = None
        self.multiline = False
        self.complete_message = True
        self.event_time = None
        self.ais_data = None
        self.header_dict = None
        self.num_lines = None
        self.group_id = None
        self.lines_in_message = []
        self.ais_dict = None
        # TODO: add some default test rounting key
        self.routing_key = os.getenv("MOV_KEY", None)
        self.event = None

    def parse_line(self, parsed_line: AIS_Line):
        if self.complete_message:
            self._reset()

        event_time = parsed_line.header_dict.get("c", datetime.datetime.now().strftime('%s'))

        if not self.event:
            if event_time:
                self.event_time = datetime.datetime.fromtimestamp(
                    int(event_time.split("*")[0]) / TIMESTAMP_DIVISOR,
                    datetime.timezone.utc,
                ).isoformat(timespec="seconds")

        if event_time is None:
            # This is to provide a best guess at event time when one isn't provided
            event_time = datetime.datetime.utcnow().isoformat()

        if not self.ais_data:
            self.ais_data = parsed_line.ais_data
            self.header_dict = parsed_line.header_dict
            self.num_lines = int(parsed_line.num_lines)
            self.group_id = parsed_line.group_id
            self.multiline = self.num_lines > 1
        elif isinstance(self.ais_data, str):
            self.ais_data = [self.ais_data, parsed_line.ais_data]
            self.header_dict = [self.header_dict, parsed_line.header_dict]
        else:
            self.ais_data.append(parsed_line.ais_data)
            self.header_dict.append(parsed_line.header_dict)

        self.lines_in_message.append(parsed_line.num_this_line)

        self.complete_message = self.num_lines == parsed_line.num_this_line
        if self.complete_message:
            self._set_ais_dict()

    def _set_ais_dict(self):
        self.ais_dict = {
            "ais": self.ais_data,
            "header": self.header_dict,
            "server_time": datetime.datetime.utcnow().isoformat(),
            "event_time": self.event_time,
            "routing_key": self.routing_key,
            "multiline": self.multiline,
            "msg_id": self.group_id,
        }


class AIS_Parser:
    """
    AIS message object definition.

    Parameters
    ----------
    msg_chunk: list (or any iterable)
        List of AIS messages str lines
    """

    # This takes a chunk of raw AIS+metadata messages and then fires off a
    #   dictionary of encoded
    # AIS message + parsed metadata to rabbit.
    # Parsing style depends on the source of data and is controlled by the
    #  .env vars

    def __init__(self):
        self.routing_key = os.getenv("MOV_KEY")
        log.debug("Sending to R-Key: {0}".format(self.routing_key))
        self.last_chunk = ""
        self.complete_message = True

    def decode_chunk(self, msg_chunk):
        if isinstance(msg_chunk, bytes):
            msg_chunk = msg_chunk.decode("utf-8")
        if isinstance(msg_chunk, str):
            msg_chunk = msg_chunk.split("\r")
            msg_chunk = [f.split("\n") for f in msg_chunk if f]
            msg_chunk = [
                item for sublist in msg_chunk for item in sublist if item
            ]

        if not isinstance(msg_chunk, List):
            msg_chunk = [msg_chunk]

        return [f for f in msg_chunk if f != "\\"]

    def check_if_line_complete(self, line):
        if self.last_chunk:
            return False
        regex_match = r"(.*)\!..VD(.*?)[^_]\*[^_][^_]"
        return bool(re.match(regex_match, line))

    def handle_incomplete_line(self, line):
        if not self.check_if_line_complete(line):
            if self.last_chunk:
                line = self.last_chunk + line
                self.last_chunk = ""
            else:
                self.last_chunk = line
                return None
        return line

    def parsing_chunk(self, msg_chunk: List, message: AIS_Message):
        msg_chunk = self.decode_chunk(msg_chunk)
        chunk_size = len(msg_chunk)

        ais_dict_list = []
        index = 0

        while index < chunk_size:
            line = msg_chunk[index]
            index += 1

            line = self.handle_incomplete_line(line)
            if not line:
                continue
            parsed_line = AIS_Line(line)
            message.parse_line(parsed_line)

            if not message.complete_message:
                # We subtract one because the indexing starts at 1 in AIS
                #   messages.
                for m in range(message.num_lines - 1):
                    if index >= chunk_size:
                        continue
                    line = msg_chunk[index]
                    index += 1

                    line = self.handle_incomplete_line(line)
                    if not line:
                        continue

                    parsed_line = AIS_Line(line)
                    message.parse_line(parsed_line)

            if message.complete_message:
                ais_dict_list.append(message.ais_dict)

        return ais_dict_list

        # IMIS metadata format.
        # \s:CSIR_000,q:u,c:1620731505,i:|X=0|D=1|T=44327.4781489815|P=10.0.100.6:12113|R=IN|E=10000000000000000000|*48\!AIVDM,1,1,,B,33ku82U000OGsfHH:Uv`9j3J0>@<,0*68
        # \g:1-2-1159,s:CSIR,c:1620731662,i:|X=1|D=1|T=44327.4683069097|P=10.0.100.6:12113|R=IN|*46\!AIVDM,2,1,9,A,53Fted42?II@D5=:2204h8Ub2222222222222216:`?1>5D80B0hDh@S0CPh,0*3F
        # \g:2-2-1159*51\!AIVDM,2,2,9,A,H8888888880,2*5D
        # s: Source
        # q:
        # c: unix timestamp
        # i: IMIS metadata
        # X: Data Source?
        # D: Delay flag?
        # T: IMIS timestamp?
        # P: IP Address of source
        # R: Direction of message?

        # Bog Standard metadata format
        # !ABVDM,1,1,,B,13=fod0vQv1B5LgdH:vMAJdB00Sa,0*42
        # !BSVDM,2,1,8,B,5E@;DN02AO;3<HMOJ20Lht84j1A9E=B222222216O@a@M00HtGQSl`3lQ1DT,0*75
        # !BSVDM,2,2,8,B,p8888888880,2*7E
        # !ABVDM,1,1,,B,HF<nO80d4v0HtpN0pvs40000000,2*62
        # !ABVDM,1,1,,B,B8u:Qa0000DwdMs8?LrDio053P06,0*59

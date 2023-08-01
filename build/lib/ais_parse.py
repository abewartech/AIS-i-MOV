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
import shutil
import tempfile
import traceback
from multiprocessing import Pool

# from re import X
import rarfile

log = logging.getLogger("ais_parse")

TIMESTAMP_DIVISOR = 1000  # because the timestamp is in milliseconds


class AIS_Parser:
    # This takes a chunk of raw AIS+metadata messages and then fires off a dictionary of encoded
    # AIS message + parsed metadata to rabbit.
    # Parsing style depends on the source of data and is controlled by the .env vars
    def __init__(self):
        self.routing_key = os.getenv("MOV_KEY")
        self.ais_meta_style = os.getenv("AIS_STYLE")
        log.debug("Sending to R-Key: {0}".format(self.routing_key))
        log.debug("AIS Style: {0}".format(self.ais_meta_style))
        self.multi_msg_dict = {}
        self.last_chunk = ""

    def read_file_lines(self, file_io):
        with open(file_io, "r") as file:
            content = file.readlines()
        return content

    def parse_line(self, line):
        _, header, ais_data = line.split("\\")
        header_items = header.strip("\\").split(",")
        header_dict = {
            item.split(":", 1)[0]: item.split(":", 1)[1]
            for item in header_items
        }
        file_line_parts = [
            f.split(",") for f in line.replace("\n", "").split("\\") if f
        ]
        timestamp, number_of_lines, number_on_this_line, message_id = (
            file_line_parts[0][0],
            int(file_line_parts[1][1]),
            file_line_parts[1][2],
            file_line_parts[1][3],
        )
        timestamp_seconds = timestamp.rsplit(":", 1)[-1].split("*", 1)[0]
        event_time_iso = datetime.datetime.fromtimestamp(
            int(timestamp_seconds) / TIMESTAMP_DIVISOR, datetime.timezone.utc
        ).isoformat(timespec="seconds")
        return (
            ais_data,
            header_dict,
            number_of_lines,
            message_id,
            event_time_iso,
        )

    def parse_ais(self, file_content, message_format=None):
        """
        Parses AIS data from the given file content.
        """
        MIN_LINE_LENGTH = 10

        ais_dict_list = []
        index = 0
        while index < len(file_content):
            line = file_content[index]
            if len(line) < MIN_LINE_LENGTH:
                index += 1
                continue

            (
                ais_data,
                meta_dict,
                number_of_lines,
                message_id,
                event_time_iso,
            ) = self.parse_line(line)

            multiline = number_of_lines > 1

            if multiline:
                for m in range(number_of_lines - 1):
                    index += 1
                    line = file_content[index]

                    if len(line) < MIN_LINE_LENGTH:
                        continue
                    ais_data_next, meta_dict_next, _, _, _ = self.parse_line(
                        line
                    )
                    if isinstance(ais_data, str):
                        ais_data = [ais_data, ais_data_next]
                        meta_dict = [meta_dict, meta_dict_next]
                    else:
                        ais_data.append(ais_data_next)
                        meta_dict.append(meta_dict_next)

            event_time = event_time_iso

            ais_dict = {
                "ais": ais_data,
                "header": meta_dict,
                "server_time": datetime.datetime.utcnow().isoformat(),
                "event_time": event_time,
                "routing_key": None,
                "multiline": multiline,
                "msg_id": message_id,
            }
            ais_dict_list.append(ais_dict)

            index += 1

        return ais_dict_list

    def process_file(self, file_path):
        """
        Process a single file. Extracts contents if its a rar file, process and delete extracted file.
        """
        COMPRESSED_FORMATS = ["rar"]
        NMEA_FILE_FORMATS = ["nmea"]
        processed_data = []

        file_extension = file_path.split(".")[-1]

        if file_extension in COMPRESSED_FORMATS:
            with rarfile.RarFile(file_path, "r") as compressed_file:
                # Each rar file is expected to have exactly one file
                extracted_file_name = compressed_file.namelist()[0]
                temp_dir = tempfile.mkdtemp()
                extracted_file_path = os.path.join(
                    temp_dir, extracted_file_name
                )
                compressed_file.extract(extracted_file_name, path=temp_dir)

                file_path = extracted_file_path
        file_extension = file_path.split(".")[-1]
        if file_extension in NMEA_FILE_FORMATS:
            file_content = self.read_file_lines(file_path)
            processed_data = self.parse_ais(file_content)

        if file_extension in COMPRESSED_FORMATS:
            # deletes the temp directory and its contents
            shutil.rmtree(temp_dir)

        return processed_data

    def process_files_in_folder(self, folder_path):
        """
        Process files in a given folder using multiprocessing. Number of workers equal to CPU count.
        """

        file_paths = [
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
        ]
        MAX_WORKERS = os.getenv("MAX_WORKERS", os.cpu_count())
        num_processes = min(os.cpu_count(), MAX_WORKERS)
        with Pool(processes=num_processes) as pool:
            all_processed_data = pool.map(self.process_file, file_paths)
        return all_processed_data

    def parse_and_seperate(self, msg_chunk, data_logger):
        # Take a chunk of messages and split them up line by line
        # Log incoming messages
        # Parse out the header and footer info
        # group multiline messages
        # return a list of dicts

        # Example Data
        # !ABVDM,1,1,,B,13=fod0vQv1B5LgdH:vMAJdB00Sa,0*42
        # !BSVDM,2,1,8,B,5E@;DN02AO;3<HMOJ20Lht84j1A9E=B222222216O@a@M00HtGQSl`3lQ1DT,0*75
        # !BSVDM,2,2,8,B,p8888888880,2*7E
        # !ABVDM,1,1,,B,HF<nO80d4v0HtpN0pvs40000000,2*62
        # !ABVDM,1,1,,B,B8u:Qa0000DwdMs8?LrDio053P06,0*59

        # Place start of message at start of chunk
        # log.debug(msg_chunk)
        msg_chunk = msg_chunk.decode("utf-8")

        # This probably needs a little more thinking. How to index chunks when there are different metadata styles?
        # How can I guarentee that the line is started in the right place?
        if self.ais_meta_style == "IMIS":
            if msg_chunk[0:3] == "\\g:" or msg_chunk[0:3] == "\\s:":
                chunk_list = msg_chunk.split("\r\n")

                # Check if last message is complete. If not then drop it so that it gets handled by
                # next incomplete starter.
                regex_match = r"\!..VD(.*?)[^_]\*[^_][^_]"
                if bool(re.match(regex_match, chunk_list[-1])) is False:
                    log.debug(
                        "Incomplete AIS message in chunk, dropping last message: {0}".format(
                            chunk_list[-1]
                        )
                    )
                    chunk_list = chunk_list[0:-1]
            else:
                prev_s = self.last_chunk.rfind("\\s")
                prev_g = self.last_chunk.rfind("\\g")
                prev_msg = self.last_chunk[max(prev_s, prev_g) :]
                chunk_list = (prev_msg + msg_chunk).split("\r\n")

                # Check if last message is complete. If not then drop it so that it gets handled by
                # next incomplete starter.
                regex_match = r"\!..VD(.*?)[^_]\*[^_][^_]"
                if bool(re.match(regex_match, chunk_list[-1])) is False:
                    log.debug(
                        "Incomplete AIS message in chunk, dropping last message: {0}".format(
                            chunk_list[-1]
                        )
                    )
                    chunk_list = chunk_list[0:-1]
                log.debug(
                    "Combining prev chunk with this chunk: {0}".format(
                        chunk_list[0]
                    )
                )
            # msg_chunk = msg_chunk[msg_chunk.index('\\s'):]

        # else self.ais_meta_style == 'None':
        #     msg_chunk = msg_chunk[msg_chunk.index('!'):]
        #     chunk_list = msg_chunk.split('\n')
        else:
            # Split the chunk into a list of messages
            msg_chunk = msg_chunk[msg_chunk.index("!") :]
            chunk_list = msg_chunk.split("\n")
        msg_dict_list = []

        for msg in chunk_list:
            try:
                if len(msg) < 2:
                    continue
                else:
                    data_logger.debug(msg)
                    msg_dict = {}
                    msg_dict = self.style_parse(msg)
                    msg_dict, complete_msg = self.aivdm_parse(msg_dict)
                    if complete_msg:
                        msg_dict_list.append(msg_dict)
            except:
                log.debug(
                    "-------------------------------------------------------"
                )
                log.debug(
                    "Problem while parsing AIS message: {0}".format(str(msg))
                )
                log.debug("Parsing Error:" + traceback.format_exc())
                log.debug("Dict: {0}".format(msg_dict))
                log.debug("Multi-Dict: {0}".format(self.multi_msg_dict))
                log.debug("\n".join(chunk_list))

        self.last_chunk = msg_chunk
        return msg_dict_list

    # ais_ingestor_1  | 2022-08-31 07:17:15,528 - DEBUG - ais_parse - -------------------------------------------------------
    # ais_ingestor_1  | 2022-08-31 07:17:15,529 - DEBUG - ais_parse - Problem while parsing AIS message: !AIVDM,1,1,,A,D02<HSiitB?b<`E6D0,4*73
    # ais_ingestor_1  | 2022-08-31 07:17:15,529 - DEBUG - ais_parse - Parsing Error:Traceback (most recent call last):
    # ais_ingestor_1  |   File "/usr/local/ais_i_mov/lib/ais_parse.py", line 88, in parse_and_seperate
    # ais_ingestor_1  |     msg_dict, complete_msg = self.aivdm_parse(msg_dict)
    # ais_ingestor_1  |   File "/usr/local/ais_i_mov/lib/ais_parse.py", line 103, in aivdm_parse
    # ais_ingestor_1  |     msg = msg_dict['ais']
    # ais_ingestor_1  | KeyError: 'ais'
    # ais_ingestor_1  |
    # ais_ingestor_1  | 2022-08-31 07:17:15,529 - DEBUG - ais_parse - Dict: {'server_time': '2022-08-31T07:17:15.528784', 'event_time': '', 'routing_key': 'encoded_ais.aishub.all'}

    def aivdm_parse(self, msg_dict):
        msg = msg_dict["ais"]
        complete_msg = False
        if msg.split(",")[1] == "1":
            msg_dict["multiline"] = False
            complete_msg = True

        # Check if first part of multiline message
        elif msg.split(",")[2] == "1":
            msg_dict["multiline"] = True
            self.multi_msg_dict = msg_dict
            self.multi_msg_dict["msg_id"] = msg.split(",")[3]
            complete_msg = False

        # Check if second part of multi msg
        elif msg.split(",")[2] == "2":
            # Check if second part belongs with first part
            if msg.split(",")[3] == self.multi_msg_dict["msg_id"]:
                combo_dict = self.multi_msg_dict
                combo_dict["ais"] = (self.multi_msg_dict["ais"], msg)
                combo_dict["header"] = (
                    self.multi_msg_dict["header"],
                    msg_dict["header"],
                )
                combo_dict["multiline"] = True
                msg_dict = combo_dict
                self.multi_msg_dict = {}
                complete_msg = True
            else:
                log.warning("Dangling multi line message: " + str(msg))
        else:
            log.warning("Unprocessed msg: " + str(msg))

        return msg_dict, complete_msg

    def style_parse(self, msg):
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

        log.debug("Parsing: {0}".format(msg))
        parsed_line = {}
        if self.ais_meta_style == "IMIS":
            meta = msg[: msg.index("\!") + 1]
            ais = msg[msg.index("\!") + 1 :]
            meta_list = meta.strip("\\").split(",")
            meta_dict = {}
            for item in meta_list:
                meta_dict[item[: item.index(":")]] = item[
                    item.index(":") + 1 :
                ]

            parsed_line["server_time"] = datetime.datetime.utcnow().isoformat()
            parsed_line["header"] = meta_dict
            parsed_line["routing_key"] = self.routing_key
            try:
                parsed_line["event_time"] = datetime.datetime.fromtimestamp(
                    int(meta_dict["c"])
                ).isoformat()
            except:
                log.debug("No timestamp on this message")
            parsed_line["ais"] = ais

        else:
            parsed_line["ais"] = msg
            parsed_line["header"] = None
            parsed_line["server_time"] = datetime.datetime.utcnow().isoformat()
            parsed_line["event_time"] = datetime.datetime.utcnow().isoformat()
            parsed_line["routing_key"] = self.routing_key

        return parsed_line

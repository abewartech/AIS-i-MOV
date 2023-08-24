#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 15:05:19 2017

@author: rory
"""
import argparse
import logging
import os
import shutil
import socket
import sys
import tempfile
import time
import traceback
from multiprocessing import Pool

import lib.ais_parse
import lib.funcs
import lib.rabbit
import rarfile
from lib.nmae_reader import stream_file_per_chunk

# This whole thing is supposed to take the raw AIS, do a
# high level split on it:
# - Meta-data
# - Raw AIS
# - Header/Footer
# - Routing key
# This is then written to a log file and passed to rabbit.


def WatchDogHandler():
    log.error("No messages published within 300 seconds. Exiting container...")
    os._exit(1)


log = logging.getLogger("main")


def setup_logging():
    """
    Setup some nice logging to store incoming data
    """
    logger = logging.getLogger(os.getenv("LOG_NAME"))
    if not len(logger.handlers):
        logger.setLevel(logging.INFO)
        log_path = os.getenv("LOG_DIR")
        log_file = os.getenv("LOG_NAME")
        os.makedirs(log_path, exist_ok=True)
        log_file_handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(log_path, log_file), when="midnight"
        )
        log_file_handler.setFormatter(
            logging.Formatter("%(asctime)s: %(message)s")
        )
        log_file_handler.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
    return logger


def process_file(file_path):
    """
    Process a single file. Extracts contents if its a rar file, process
        and delete extracted file.
    """
    COMPRESSED_FORMATS = ["rar"]
    NMEA_FILE_FORMATS = ["nmea"]
    original_file_path = file_path
    file_extension = file_path.split(".")[-1]
    # Create RabbitMQ publisher
    rabbit_publisher = lib.rabbit.DockerRabbitProducer()
    ais_parser = lib.ais_parse.AIS_Parser()
    ais_message = lib.ais_parse.AIS_Message()

    if file_extension in COMPRESSED_FORMATS:
        with rarfile.RarFile(file_path, "r") as compressed_file:
            # Each rar file is expected to have exactly one file
            extracted_file_name = compressed_file.namelist()[0]
            temp_dir = tempfile.mkdtemp()
            extracted_file_path = os.path.join(temp_dir, extracted_file_name)
            compressed_file.extract(extracted_file_name, path=temp_dir)

            file_path = extracted_file_path
    file_extension = file_path.split(".")[-1]
    if file_extension in NMEA_FILE_FORMATS:
        for chunk in stream_file_per_chunk(file_path):
            msg_list = ais_parser.parsing_chunk(chunk, ais_message)

            for msg in msg_list:
                try:
                    log.debug("Sending message to RMQ: " + str(msg))
                    rabbit_publisher.produce(msg, msg["routing_key"])
                except Exception as e:
                    error_message = f"Failed to send msg to rabbitmq:{e}"
                    log.error(error_message)
    log.info("File parsed: {file_path}")
    if file_extension in COMPRESSED_FORMATS:
        # deletes the temp directory and its contents
        shutil.rmtree(temp_dir)
    shutil.rmtree(original_file_path)
    log.info("File deleted: {file_path}")


def process_files_in_folder(folder_path):
    """
    Process files in a given folder using multiprocessing. Number of workers
        equal to CPU count.
    """
    file_paths = [
        os.path.join(folder_path, file)
        for file in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, file))
    ]
    file_paths = [f for f in file_paths if "gitignore" not in f]
    if len(file_paths) == 0:
        return
    MAX_WORKERS = os.getenv("MAX_WORKERS", os.cpu_count())
    num_processes = min(os.cpu_count(), MAX_WORKERS)
    num_processes = min(len(file_paths), num_processes)

    with Pool(processes=num_processes) as pool:
        pool.map(process_file, file_paths)
    return


def read_files(files_folder):
    try:
        while True:
            process_files_in_folder(files_folder)
    except Exception as e:
        log.error("Error reading from file:")
        log.error(e)
        log.error(traceback.format_exc())
    return


def read_socket(data_logger):
    # Create a TCP/IP socket
    log.info("Opening socket")
    # TODO: check how to read this from the files
    sock = socket.socket()
    log.info(
        "Setting socket timeout to %s seconds.", os.getenv("SOCKET_TIMEOUT")
    )
    sock.settimeout(int(os.getenv("SOCKET_TIMEOUT")))
    server_address = (os.getenv("SOURCE_HOST"), int(os.getenv("SOURCE_PORT")))
    log.info("Connecting to " + str(server_address))
    sock.connect(server_address)
    # # Create RabbitMQ publisher
    rabbit_publisher = lib.rabbit.Rabbit_Producer()
    ais_parser = lib.ais_parse.AIS_Parser()
    ais_message = lib.ais_parse.AIS_Message()

    # watchdog = lib.funcs.Watchdog(300, WatchDogHandler)
    try:
        log.info("Streaming AIS...")
        while True:
            # https://stackoverflow.com/questions/47758023/python3-socket-random-partial-result-on-socket-receive
            while True:
                try:
                    # Check the chunk
                    chunk = sock.recv(int(os.getenv("CHUNK_BYTES")))

                    log.debug("Chunk received")

                    if not chunk:
                        log.debug(
                            "Complete chunk, reading more: len = {}".format(
                                len(chunk)
                            )
                        )
                        break

                    # Parse the said chunk.
                    # The parser will only return a list of complete messages
                    # (multiline or not)
                    # The remaining, will it be only a piece of a line, or an
                    # incomplete multiline message, it stays in memory
                    # waiting for the remaing.
                    msg_list = ais_parser.parsing_chunk(chunk, ais_message)

                    log.debug("Chunk parsed")

                    # send to rabbitMQ
                    for msg in msg_list:
                        try:
                            log.debug("Sending message to RMQ: " + str(msg))
                            rabbit_publisher.produce(msg, msg["routing_key"])
                        except Exception as e:
                            error_message = (
                                f"Failed to send msg to rabbitmq:{e}"
                            )
                            log.error(error_message)
                except socket.error:
                    sock.close()
                    break
            log.debug("----------------")
    except Exception as e:
        log.error(e)
        log.error("Error in AIS streaming:" + traceback.format_exc())
        log.error("Last MSG: " + str(msg))
    finally:
        log.warning("Closing socket")
        # TODO why no watchdog stop? why do timsleep where?
        sock.close()
        time.sleep(10)


def do_work(folder=None):
    """
    This opens a socket to the server that is feeding AIS.
    Collect multiline messages into a single dict.
    Adds a routing key, server time etc to the message
    Publishes message onto RabbitMQ
    """
    log.info("Getting ready to do work...")
    data_logger = setup_logging()
    if folder:
        log.info("Reading from files.")
        read_files(folder)
    else:
        log.info("No file folder provided.")
        try:
            while True:
                read_socket(data_logger)
        except Exception as e:
            log.error(f"Error in main loop:{e}")
            log.error(traceback.format_exc())
            time.sleep(10)

    log.info("Worker shutdown...")


def main(args):
    """
    Setup logging, and args, then "do_work"
    """
    start = time.time()
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        level=getattr(logging, args.loglevel),
    )

    log.setLevel(getattr(logging, args.loglevel))
    log.info("ARGS: {0}".format(args))
    folder = args.folder
    do_work(folder)
    end = time.time()
    log.info("The work took:")
    log.info(end - start)
    log.warning("Script Ended...")


if __name__ == "__main__":
    """
    This takes the command line args and passes them to the 'main' function
    """
    PARSER = argparse.ArgumentParser(description="Run the DB inserter")
    PARSER.add_argument(
        "-f",
        "--folder",
        help="This is the folder to read.",
        default=None,
        required=False,
    )
    PARSER.add_argument(
        "-ll",
        "--loglevel",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set log level for service (%s)" % "INFO",
    )
    ARGS = PARSER.parse_args()
    try:
        main(ARGS)
    except KeyboardInterrupt:
        log.warning("Keyboard Interrupt. Exiting...")
        os._exit(0)
    except Exception as error:
        log.error("Other exception. Exiting with code 1...")
        log.error(traceback.format_exc())
        log.error(error)
        os._exit(1)

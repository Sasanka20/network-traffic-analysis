#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Run script as following:

>>> python process_data.py -D /path/to/data.csv
"""


import os
import sys
import datetime
import logging
import optparse


def get_logger(debug: bool) -> object:
    """Get logger object to use logging in defined format."""

    logger = logging.getLogger()
    fmt = '%(asctime)s.%(msecs).03d %(process)+5s %(levelname)-8s %(filename)s:%(lineno)d:%(funcName)s(): %(message)s'

    logger.handlers = []
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging.Formatter(fmt, '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(stream_handler)

    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger


class ProcessData():

    def __init__(self, logger: object):
        self.logger = logger
        self.csv_header = 'timestamp,destination_ip,protocol_name,total_packets,total_bytes'
        self.columns_to_get = [
            'Timestamp',
            'Destination.IP',
            'ProtocolName',
            'Total.Fwd.Packets',
            'Total.Backward.Packets',
            'Init_Win_bytes_forward',
            'Init_Win_bytes_backward',
        ]

    def process_data(self, data_path: str, save_data_folder_path=None) -> None:
        """Process data saved in data_path to make a new aggregation."""
        with open(data_path, 'r') as file_read:
            for line_num, line in enumerate(file_read):
                line_splitted = line.strip().split(',')

                if line_num == 0:
                    list_of_indices = self.get_list_of_indices(line_splitted)
                    self.logger.info('Parsed list of indices: %s', list_of_indices)
                    continue

                list_of_values = [line_splitted[i] if i < len(line_splitted) else None for i in list_of_indices]

                if None in list_of_values:
                    self.logger.warning('Following list_of_values contains None: %s', list_of_values)
                    continue

                save_data_file_name, joined_values = self.get_file_name_and_values(list_of_values)
                self.save_values_to_csv(save_data_file_name, joined_values, save_data_folder_path)

                if line_num % 10_000 == 0:
                    self.logger.info('Processed %s lines.', line_num)

    def get_list_of_indices(self, line_splitted: list) -> list:
        """Get list of values indices that will be needed."""
        list_of_indices = []

        for col in self.columns_to_get:
            if col in line_splitted:
                list_of_indices.append(line_splitted.index(col))
            else:
                list_of_indices.append(None)

        return list_of_indices

    @staticmethod
    def get_file_name_and_values(list_of_values: list) -> tuple:
        """Get CSV file name and string of values delimited by comma as one line in CSV file."""
        timestamp, dest_ip, protocol_name, fwd_packets, bwd_packets, fwd_bytes, bwd_bytes = list_of_values

        # get day-hour format from timestamp
        timestamp_dt = datetime.datetime.strptime(timestamp, '%d/%m/%Y%H:%M:%S')
        timestamp_str = datetime.datetime.strftime(timestamp_dt, '%Y%m%d%H%M%S')
        timestamp_name = datetime.datetime.strftime(timestamp_dt, '%Y%m%d%H')

        # copmute instance statistics
        total_packets = str(int(fwd_packets) + int(bwd_packets))
        total_bytes = str(int(fwd_bytes) + int(bwd_bytes))

        return (
            f'{timestamp_name}.csv',
            ','.join([timestamp_str, dest_ip, protocol_name, total_packets, total_bytes])
        )

    def save_values_to_csv(self, save_data_file_name: str, joined_values: str, save_data_folder_path=None) -> None:
        """Save joined_values as one line to save_data_file_name in current directory."""

        csv_file_path = os.path.join(save_data_folder_path or os.getcwd(), save_data_file_name)

        if save_data_file_name not in os.listdir(save_data_folder_path):
            with open(csv_file_path, 'w') as file_write:
                file_write.write(self.csv_header + '\n')

        with open(csv_file_path, 'a') as file_append:
            file_append.write(joined_values + '\n')


def main():
    start_time = datetime.datetime.now()

    # parse script options
    usage = f'{sys.argv[0]}: see source code'
    parser = optparse.OptionParser(usage, version='{} version 0.1'.format(sys.argv[0]))
    parser.add_option('-D', '--data', action='store', dest='data_path', help='data path')
    parser.add_option('-B', '--debug', action='store_true', dest='debug', default=None, help='entry debug mode')
    opts, _ = parser.parse_args()

    # set logger
    logger = get_logger(opts.debug)

    # get instance of PreprocessData
    process_data = ProcessData(logger)
    process_data.process_data(opts.data_path)

    logger.info('Finished processing in %s.', datetime.datetime.now() - start_time)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass


# vim: set cin et ts=4 sw=4 ft=python :

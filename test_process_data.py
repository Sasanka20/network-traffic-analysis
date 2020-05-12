#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from process_data import ProcessData, get_logger

logger = get_logger(debug=False)
process_data = ProcessData(logger)

TEST_RAW_DATA_ROWS = [
    'Timestamp,Destination.IP,ProtocolName,Total.Fwd.Packets,Total.Backward.Packets,Init_Win_bytes_forward,Init_Win_bytes_backward',
    '26/04/201711:11:17,10.200.7.7,HTTP_PROXY,22,55,256,490',
    '26/04/201711:11:17,172.19.1.46,HTTP_PROXY,2,0,490,-1',
    '26/04/201711:11:17,10.200.7.217,HTTP,3,0,888,-1',
    '',
    'wrong_data,1,HTTP',
    '26/04/201711:11:17,10.200.7.217,HTTP,1,3,888,490',
    '26/04/201711:11:17,10.200.7.7,HTTP_PROXY,5,0,253,-1'
]
TEST_LINE_SPLITTED_1 = [
    'Flow.ID',
    'Source.IP',
    'Source.Port',
    'Destination.IP',
    'Destination.Port',
    'Protocol',
    'Timestamp',
    'ProtocolName',
    'Total.Fwd.Packets',
    'Total.Backward.Packets',
    'Init_Win_bytes_forward',
    'Init_Win_bytes_backward',
]
TEST_LINE_SPLITTED_2 = [
    'Flow.ID',
    'Destination.IP',
    'Destination.Port',
    'Timestamp',
    'ProtocolName',
    'Total.Fwd.Packets',
    'Total.Backward.Packets',
]
TEST_LIST_OF_VALUES = [
    '26/4/201711:17:45',
    '10.200.7.217',
    'HTTP',
    '1',
    '2',
    '800',
    '87'
]
TEST_SAVE_DATA_FILE_NAME = '2020050615.csv'
TEST_JOINED_VALUES_1 = '2020050615,10.200.7.7,HTTP_PROXY,5,252'
TEST_JOINED_VALUES_2 = '2020050615,34.213.8.1,YOUTUBE,6,100'


def test_process_data(tmpdir):
    raw_data_path = os.path.join(tmpdir, 'raw_data.csv')
    with open(raw_data_path, 'w') as file_write:
        for row in TEST_RAW_DATA_ROWS:
            file_write.write(row + '\n')

    process_data.process_data(raw_data_path, tmpdir)
    assert '2017042611.csv' in os.listdir(tmpdir)

    save_data_file_path = os.path.join(tmpdir, '2017042611.csv')
    with open(save_data_file_path, 'r') as file_read:
        lines = [line.strip() for line in file_read]
        assert lines == [
            'timestamp,destination_ip,protocol_name,total_packets,total_bytes',
            '20170426111117,10.200.7.7,HTTP_PROXY,77,746',
            '20170426111117,172.19.1.46,HTTP_PROXY,2,489',
            '20170426111117,10.200.7.217,HTTP,3,887',
            '20170426111117,10.200.7.217,HTTP,4,1378',
            '20170426111117,10.200.7.7,HTTP_PROXY,5,252'
        ]


def test_get_list_of_indices():
    list_of_indices_all_cols = process_data.get_list_of_indices(TEST_LINE_SPLITTED_1)
    assert list_of_indices_all_cols == [6, 3, 7, 8, 9, 10, 11]

    list_of_indices_missing_cols = process_data.get_list_of_indices(TEST_LINE_SPLITTED_2)
    assert list_of_indices_missing_cols == [3, 1, 4, 5, 6, None, None]


def test_get_file_name_and_values():
    file_name, joined_values = process_data.get_file_name_and_values(TEST_LIST_OF_VALUES)
    assert file_name == '2017042611.csv'
    assert joined_values == '20170426111745,10.200.7.217,HTTP,3,887'


def test_save_values_to_csv(tmpdir):
    save_data_file_path = os.path.join(tmpdir, TEST_SAVE_DATA_FILE_NAME)

    process_data.save_values_to_csv(TEST_SAVE_DATA_FILE_NAME, TEST_JOINED_VALUES_1, tmpdir)
    with open(save_data_file_path, 'r') as file_read_1:
        num_lines_after_making_new_file = sum(1 for line in file_read_1)
    assert num_lines_after_making_new_file == 2

    process_data.save_values_to_csv(TEST_SAVE_DATA_FILE_NAME, TEST_JOINED_VALUES_2, tmpdir)
    with open(save_data_file_path, 'r') as file_read_2:
        num_lines_after_adding_second_instance = sum(1 for line in file_read_2)
    assert num_lines_after_adding_second_instance == 3

# vim: set cin et ts=4 sw=4 ft=python :

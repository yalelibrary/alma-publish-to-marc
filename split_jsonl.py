#!/usr/bin/python3
import os
import argparse

parser = argparse.ArgumentParser(prog='marc_split', description='Split JSONL file in smaller JSONL files')
parser.add_argument('inputfile', nargs=1, help='Input JSONL file')
parser.add_argument('-x', '--export-directory', help='Output directory', default=None)
parser.add_argument('-p', '--file-prefix', help='Output filename prefix', default=None)
parser.add_argument('-n', '--num', help='Number per file', default=1000)
args = parser.parse_args()

num_per_file = int(args.num)
output_directory = args.export_directory if args.export_directory else os.path.dirname(args.inputfile[0])
filename_prefix = args.file_prefix if args.file_prefix else args.file_prefix[0]

with open(args.inputfile[0], 'r') as reader:
    file_ix = 0
    record_ix = 0
    output_file = open(os.path.join(output_directory, f'{filename_prefix}-{file_ix}.jsonl'), 'w')
    while True:
        line = reader.readline()
        if not line:
            break
        if record_ix >= num_per_file:
            output_file.close()
            file_ix += 1
            record_ix = 0
            output_file = open(os.path.join(output_directory, f'{filename_prefix}-{file_ix}.jsonl'), 'w')
        output_file.write(line)
        record_ix += 1
    output_file.close()

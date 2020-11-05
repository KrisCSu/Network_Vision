#!/bin/bash

tshark -r input.pcap -T json >output.json

python3 data_cleaner.py output.json data.json
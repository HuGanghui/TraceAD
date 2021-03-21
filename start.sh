#!/usr/bin/env bash
nohup python src/consumer_a.py >output_a 2>&1 &
nohup python src/consumer_b.py >output_b 2>&1 &

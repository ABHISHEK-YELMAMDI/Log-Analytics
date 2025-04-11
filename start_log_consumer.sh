#!/bin/bash
sleep 60
echo "Running db_init.py"
python db_init.py || echo "db_init.py failed with exit code $?"
echo "Starting log_consumer.py"
python log_consumer.py

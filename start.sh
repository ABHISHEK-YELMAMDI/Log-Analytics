#!/bin/bash
sleep 10
echo "Running db_init.py"
python db_init.py || echo "db_init.py failed with exit code $?"

python log_consumer.py

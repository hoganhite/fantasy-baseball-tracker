#!/bin/bash
pip install -r requirements.txt
apt-get update && apt-get install -y libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libgbm1 libasound2
python -m playwright install --with-deps chromium
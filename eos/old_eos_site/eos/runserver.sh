#!/bin/bash
echo "EOS server Started"
source .venv/bin/activate
python3 manage.py runserver 0.0.0.0:8000
echo "EOS server Closed"

#!/bin/bash
while true; do
  /usr/bin/python3 /home/rocky/jobs/googlecanonicallinks/rss2cl.py >/dev/null
  sleep 1  # Wait for 2 minutes before the next iteration
done
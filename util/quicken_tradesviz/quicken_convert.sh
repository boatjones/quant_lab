#!/bin/bash

python quicken_to_tradesviz_07.py \
  --input Rollover.txt \
  --output tradesviz_import2.csv \
  --symbols-map symbols_map.csv \
  --tz America/New_York \
  --emit-cashflows --cashflows-output cashflows.csv


#!/usr/bin/env bash

head -n1 ./data/raw/pp-complete.csv > ./data/raw/pp_test.csv
tail -n+2 ./data/raw/pp-complete.csv | shuf -n 25 >> ./data/pp_test.csv

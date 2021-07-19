#!/usr/bin/env bash

head -n1 pp-complete.csv > pp_sample.csv
tail -n+2 pp-complete.csv | shuf -n 250000 >> pp_sample.csv

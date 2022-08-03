#!/usr/bin/bash

# execute poisson rate submission job and output as json with latency bins
fio etc-fio/poisson-rate-submission.ini --output-format=json+ --output="results/$(date -Imin)_plus_poisson-submit.json" --unified_rw_reporting=both

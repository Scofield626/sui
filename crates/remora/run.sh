#!/bin/bash

# Define the load and proxy ranges
load_numbers=(5000 10000 15000 20000 25000 30000 35000 40000 45000 50000)

# Check if the results directory is not empty
if [ -d "./results" ] && [ "$(ls -A ./results)" ]; then
  echo "Error: The results directory is not empty. Please clear it before running the script."
  exit 1
fi

# Create the results directory if it doesn't exist
mkdir -p ./results

# Create the config directory if it doesn't exist
mkdir -p ./config

# Loop through each combination of load and proxy number
for load in "${load_numbers[@]}"; do

  # Update benchmark.json with the current load number
  echo "{\"load\": $load}" >./config/benchmark.json

  # Prepare the log file name
  log_name="load-${load}.log"

  # Launch the first process in the background
  cargo run --release --bin remora -- --benchmark-config config/benchmark.json >>./results/$log_name &
  remora_pid=$!

  # Launch the second process in the background
  cargo run --release --bin load_generator -- --benchmark-config config/benchmark.json &
  load_gen_pid=$!

  # Wait for the second process to complete
  wait $load_gen_pid

  # Once the second process completes, kill the first process
  kill $remora_pid

  echo "Run completed for load=$load. Log saved to ./results/$log_name"

done

rm ./config/*

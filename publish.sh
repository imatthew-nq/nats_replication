#!/bin/bash

# NATS server address
nats_server="nats://nats-1:4222"

# Number of messages to publish
num_messages=10000

# Subject to publish messages to
subject="my_stream.>"

# Function to generate a random number between 1 and 10000
generate_random_number() {
  echo $((1 + RANDOM % 10000))
}

for ((i=1; i<=num_messages; i++))
do
  date=$(date +"%Y-%m-%d %H:%M:%S")
  random_number=$(generate_random_number)
  message="$date this message has been created - sequence number is $random_number"
  nats pub $subject "$message" --server $nats_server
  echo "Published message $i: $message"
done

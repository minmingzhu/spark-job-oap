#!/bin/bash

# find Spark application ID from a input file, $1
# example: ./test.sh file.txt

declare -r regex=".+Connected.+(app\-[0-9]+\-[0-9]+)"

while read -r line
do
	if [[ $line =~ $regex ]]; then
		declare -r app_id=${BASH_REMATCH[1]}
		break;
	fi
done < "$1"

if [[ ! -z ${app_id+x} ]]; then
	echo "$app_id"
else
	echo "not application id found"
fi

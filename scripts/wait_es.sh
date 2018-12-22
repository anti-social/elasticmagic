#!/usr/bin/env bash

host=$1

echo " * Waiting Elasticsearch cluster: ${host} ..."
while true; do
	output=`curl -XGET "${host}/_cat/health?h=status" -s | tr -d '[[:space:]]'`
	if [ -z "${output}" ]; then
		echo -n "."
	else
		echo " > Elasticsearch status: ${output}"
	fi
	if [ "${output}" = "green" -o "${output}" = "yellow" ]; then
		break
	fi
	sleep 1;
done
echo " * Elasticsearch is ready!"

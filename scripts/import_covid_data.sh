#!/usr/bin/env bash

:<<DOC
This scripts expects to have a token defined as an environment variable, like this:
export API_TOKEN="ddNjO0sMa4_r8TM60LJKRjUlzttaGCKSIADhTt9lhhHLgA4nnvNE26FgnasqhTDiczvnf5XL2nMJZNMuTgu3Vg=="
DOC

if ! ID=$(/bin/basename "$0"); then
  echo "ERROR: Unable to figure out program name"
  exit 100
fi
if [ -z "$1" ]; then
  logger --id "$ID" --stderr "Missing url parameter"
  exit 100
fi
url=$1
if [ -z "$API_TOKEN" ]; then
  logger --id "$ID" --stderr "API_TOKEN environment variable is missing"
  exit 100
fi
dryrun=$2

if ! /usr/bin/docker --interactive --tty influx write "$dryrun" --format csv --url "$url"; then
  logger --id "$ID" --stderr "COVID URL import failed!"
  exit 100
fi
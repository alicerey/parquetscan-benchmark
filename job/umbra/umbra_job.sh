#!/usr/bin/env bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
docker run -it --privileged -v ${PWD}:/umbra -v $SCRIPT_DIR:/scripts --entrypoint /scripts/entrypoint.sh ghcr.io/alicerey/umbra-release:30908e0cd

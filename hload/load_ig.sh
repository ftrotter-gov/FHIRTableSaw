#!/bin/bash
docker run --rm -it \
  --network host \
  hapiproject/hapi-fhir-cli:latest \
  upload-ig \
  -v r4 \
  -b http://localhost:8080/fhir \
  -d ./ndh_package.tgzdocker

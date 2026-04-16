#!/bin/bash
./wyomingizer.gobin -input_dir /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_ndjson/ -output_dir /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/ -states WY --overwrite
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/Endpoint.WY.ndjson > ../data/log/Endpoint.WY.100.ndjson 
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/Practitioner.WY.ndjson > ../data/log/Practitioner.WY.100.ndjson 
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/PractitionerRole.WY.ndjson > ../data/log/PractitionerRole.WY.100.ndjson 
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/Organization.WY.ndjson > ../data/log/Organization.WY.100.ndjson 
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/OrganizationAffiliation.WY.ndjson > ../data/log/OrganizationAffiliation.WY.100.ndjson 
head -100 /Volumes/eBolt/new_ndjson_WY/internal_CMS_ETL_API_WY_ndjson/Location.WY.ndjson > ../data/log/Location.WY.100.ndjson 

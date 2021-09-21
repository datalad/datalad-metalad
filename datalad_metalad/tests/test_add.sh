#!/usr/bin/env bash

temp_dir=$(mktemp -d)
datalad create $temp_dir
id=$(datalad wtf -d $temp_dir|grep -e "- id:"|cut -d : -f 2|tr -d " ")


cat <<EOF |datalad meta-add -d $temp_dir -
{
        "type": "file",
        "path": "a/b/c",
        "extractor_name": "e1",
        "extractor_version": "v1",
        "extraction_parameter": {},
        "extraction_time": 1.1,
        "agent_name": "me",
        "agent_email": "me@you.com",
        "dataset_id": "$id",
        "dataset_version": "0000002",
        "extracted_metadata": {"meta": "something"}
}
EOF

num=$(datalad -f json meta-dump -d $temp_dir -r "*"|wc -l)
if [[ $num -ne "1" ]]; then
  echo "expected one metadata entry, got: $num"
  rm -rf $temp_dir
  exit 1
fi


cat <<EOF |datalad meta-add -d $temp_dir -
{
        "type": "file",
        "path": "d/e/f",
        "extractor_name": "e2",
        "extractor_version": "v2",
        "extraction_parameter": {},
        "extraction_time": 1.2,
        "agent_name": "me",
        "agent_email": "me@you.com",
        "dataset_id": "$id",
        "dataset_version": "0000002",
        "extracted_metadata": {"meta": "something"}
}
EOF

num=$(datalad -f json meta-dump -d $temp_dir -r "*"|wc -l)
if [[ $num -ne "2" ]]; then
  echo "expected two metadata entries, got: $num"
  rm -rf $temp_dir
  exit 1
fi


cat <<EOF |datalad meta-add -d $temp_dir -
{
        "type": "file",
        "path": "g/h/i",
        "extractor_name": "e2",
        "extractor_version": "v2",
        "extraction_parameter": {},
        "extraction_time": 1.2,
        "agent_name": "me",
        "agent_email": "me@you.com",
        "dataset_id": "$id",
        "dataset_version": "0000002",
        "extracted_metadata": {"meta": "something"}
}
EOF

num=$(datalad -f json meta-dump -d $temp_dir -r "*"|wc -l)
if [[ $num -ne "3" ]]; then
  echo "expected three metadata entries, got: $num"
  rm -rf $temp_dir
  exit 1
fi


cat <<EOF |datalad meta-add -d $temp_dir -
{
        "type": "file",
        "path": "j/k/l",
        "extractor_name": "e2",
        "extractor_version": "v2",
        "extraction_parameter": {},
        "extraction_time": 1.2,
        "agent_name": "me",
        "agent_email": "me@you.com",
        "dataset_id": "$id",
        "dataset_version": "0000002",
        "extracted_metadata": {"meta": "something"}
}
EOF

num=$(datalad -f json meta-dump -d $temp_dir -r "*"|wc -l)
if [[ $num -ne "4" ]]; then
  echo "expected four metadata entries, got: $num"
  rm -rf $temp_dir
  exit 1
fi


rm -rf $temp_dir

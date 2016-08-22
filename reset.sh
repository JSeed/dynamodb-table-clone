#!/bin/bash
SOURCE_REGION=$1
DESTINATION_REGION=$2
TABLE_NAME=$3
S3_LOCATION=$4

USAGE="$0 source_region destination_region table_name jar_s3_location"

if [ $# != 4 ]; then
  echo "Usage: $USAGE"
  exit 1
fi

echo "Running table reset"
echo "SOURCE_REGION $SOURCE_REGION - DESTINATION_REGION $DESTINATION_REGION - TABLE_NAME $TABLE_NAME - S3_Location $S3_LOCATION"

aws s3 cp $S3_LOCATION ./

SPLIT_PATH=(${S3_LOCATION//\// })
JAR_FILE=${SPLIT_PATH[${#SPLIT_PATH[@]}-1]}

java -jar $JAR_FILE --sourceRegion $SOURCE_REGION --destinationRegion $DESTINATION_REGION --tableName $TABLE_NAME --reset

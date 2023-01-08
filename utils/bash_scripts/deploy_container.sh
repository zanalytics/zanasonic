#!/bin/bash
##########################################

# environment variable AWS_DEVELOPMENT_ACCOUNT will need to be set up on your machine or in github.

# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

echo $PWD

REGION=eu-west-2
ENVIRONMENT=${1:-development}

export AWS_DEFAULT_REGION=$REGION


if [[ "$ENVIRONMENT" = "development" ]]; then
  ACCOUNT=$AWS_DEVELOPMENT_ACCOUNT
elif [[ "$ENVIRONMENT" = "qa" ]]; then
  ACCOUNT=$AWS_QA_ACCOUNT
elif [[ "$ENVIRONMENT" = "stage" ]]; then
  ACCOUNT=$AWS_STAGE_ACCOUNT
elif [[ "$ENVIRONMENT" = "production" ]]; then
  ACCOUNT=$AWS_PRODUCTION_ACCOUNT
else
  echo "No Valid Environment"
  exit 125
fi

echo "using" $ACCOUNT  "in" $ENVIRONMENT


# Log on to  ECR
# aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com

# Build the docker images and push to ecr registry
docker buildx build --platform linux/amd64 -t ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${ENVIRONMENT}/zanasonic --build-arg .
# docker push ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${ENVIRONMENT}/zanasonic:latest
echo "Success - Pushed the development container to" $ENVIRONMENT

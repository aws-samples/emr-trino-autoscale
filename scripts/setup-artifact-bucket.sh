#!/bin/bash
#==============================================================================
#!# setup-artifact-bucket.sh - Setup the required templates and scripts
#!# in a private S3 bucket.
#!#
#!#  version         1.1
#!#  author          ripani
#!#  license         MIT license
#!#
#==============================================================================
#?#
#?# usage: ./setup-artifact-bucket.sh <REGIONAL_S3_BUCKET>
#?#        ./setup-artifact-bucket.sh mybucketname
#?#
#?#  REGIONAL_S3_BUCKET            S3 bucket name eg: mybucketname
#?#
#==============================================================================
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function usage() {
	[ "$*" ] && echo "$0: $*"
	sed -n '/^#?#/,/^$/s/^#?# \{0,1\}//p' "$0"
	exit 1
}

[[ $# -ne 1 ]] && echo "error: missing parameters" && usage

S3_BUCKET="$1"
S3_PREFIX="artifacts/aws-emr-trino-autoscaling"

# build package
cd "$DIR/.." || return
sbt clean assembly

# upload install script and JARS
aws s3 rm --recursive "s3://$S3_BUCKET/$S3_PREFIX/"
aws s3 cp "$DIR/emr-install_autoscale.sh" "s3://$S3_BUCKET/$S3_PREFIX/emr-install_autoscale.sh"
aws s3 cp $DIR/../target/scala-2.12/trino-autoscaling-assembly-*.jar "s3://$S3_BUCKET/$S3_PREFIX/trino-autoscale.jar"
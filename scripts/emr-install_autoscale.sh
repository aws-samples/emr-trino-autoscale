#!/bin/bash
#==============================================================================
#!# emr-install_autoscale.sh - Install Trino Autoscale on an Amazon EMR cluster
#!#
#!#  version         1.1
#!#  author          ripani
#!#  license         MIT license
#!#
#==============================================================================
#?#
#?# usage: ./emr-install_autoscale.sh <REGIONAL_S3_BUCKET>
#?#        ./emr-install_autoscale.sh mybucketname
#?#
#?#  REGIONAL_S3_BUCKET            Amazon S3 bucket name eg: mybucketname
#?#
#==============================================================================

function usage() {
	[ "$*" ] && echo "$0: $*"
	sed -n '/^#?#/,/^$/s/^#?# \{0,1\}//p' "$0"
	exit 1
}

# make sure to run as root
if [ $(id -u) != "0" ]; then
	sudo "$0" "$@"
	exit $?
fi

[[ $# -ne 1 ]] && echo "error: missing parameters" && usage

S3_BUCKET="$1"
S3_PREFIX="artifacts/aws-emr-trino-autoscaling"
INSTALL_PATH="/opt/trino-autoscale"

# create folder
mkdir -p $INSTALL_PATH

# download packages
aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/trino-autoscale.jar" "$INSTALL_PATH/trino-autoscale.jar"

# systemd service
cat << EOF > /etc/systemd/system/trino-autoscale.service
[Unit]
Description=Trino-Autoscale

[Service]
Environment=JAVA_HOME=/etc/alternatives/jre
Type=forking
ExecStart=/usr/bin/trino-autoscale
Restart=always
RestartSec=5
PIDFile=/var/run/trino/trino-autoscale.pid
User=trino
WorkingDirectory=$INSTALL_PATH


[Install]
WantedBy=multi-user.target
EOF

# create startup script
cat << 'EOF' > /usr/bin/trino-autoscale
#!/bin/bash -l
#
# Copyright 2008-2009 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
#

PID_FILE=/var/run/trino/trino-autoscale.pid

TRINO_AUTOSCALE_CP="/opt/trino-autoscale/trino-autoscale.jar"

TRINO_JAVA=$JAVA_HOME
if [ -d "/usr/lib/jvm/java-7-oracle" ]; then
   TRINO_JAVA=/usr/lib/jvm/java-7-oracle
elif [ -h "/usr/java/latest" ]; then
   TRINO_JAVA=/usr/java/latest
fi

export LANG=en_US.UTF-8

$TRINO_JAVA/bin/java -Xmx1024m -XX:+ExitOnOutOfMemoryError -XX:MinHeapFreeRatio=10 -server -cp $TRINO_AUTOSCALE_CP com.amazonaws.emr.TrinoAutoscaler 2>> /var/log/trino/trino-autoscale.out  2>&1 &

JAVA_PID=$!
echo $JAVA_PID > $PID_FILE

# wait for autoscale to start up, grab the lock file and write itself to it
echo "$(date '+%Y-%m-%d %H:%M:%S') Trino Autoscale Process started with pid $JAVA_PID. Now waiting for process PID file"
for I in {1..10} ; do
    PID_IN_FILE=$(cat $PID_FILE 2>/dev/null)
    if [ "$PID_IN_FILE" != "$JAVA_PID" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Trino Autoscale PID_FILE $PID_FILE doesn't exist or doesn't contain $JAVA_PID (contains '$PID_IN_FILE')... sleeping to give process a chance to start"
        sleep 5
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') Trino Autoscale Process Started"
      exit 0
    fi
done

echo "$(date '+%Y-%m-%d %H:%M:%S') Trino Autoscale PID_FILE contains $PID_IN_FILE != $JAVA_PID. Start failed."
exit 1

EOF

# Fix permissions
chmod +x /usr/bin/trino-autoscale
chmod oug+wrx /emr/metricscollector/isbusy
chown trino:trino /emr/metricscollector/isbusy

# Start service
systemctl daemon-reload
systemctl enable trino-autoscale.service
systemctl start trino-autoscale.service


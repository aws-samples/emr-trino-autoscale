## Application Logging

The autoscaler generates logs within the Trino log path on the EMR master node: */var/log/trino/trino-autoscale.log*

Logs files are rotated every hour and are automatically pushed on Amazon S3 if you enabled the [EMR Cluster Logging](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-debugging.html).

When S3 logging is enabled, files are retained in the EMR master node for 2 days after they are successfully shipped to S3 as defined in the EMR logpusher configuration for Trino: */etc/logpusher/trino.config*

# Amazon EMR - Trino Autoscale

This project provides a custom auto-autoscaling for Amazon EMR on EC2 clusters running with Trino.
The package support out of the box Instance Groups and Instance Fleets clusters with On Demand / SPOT instances.

## Features

* Scaling based on cluster CPU utilization
* Scaling hints using Trino required_workers SESSION parameter
* Metrics collection using Trino JMX REST API
* Amazon CloudWatch integration
* Support for Instance Fleets and Instance Groups EMR clusters
* Support for On Demand and SPOT instance types
* Concurrent Scaling for Instance Groups clusters
* EMR multi-master supported
* Support EMR clusters with Kerberos 
* Support EMR clusters with Trino Ranger plugin

## Documentation

- [Configurations](./docs/configurations.md) Configurations for controlling autoscaling behaviour
- [Fault-Tolerant](./docs/fault-tolerant.md) Sample configurations for Trino fault tolerant execution
- [Logging](./docs/logging.md) Application Logging details
- [Scaling Algorithm](./docs/scaling-logic.md) Description of scaling behaviour
- [Scaling Hints](./docs/scaling-hints.md) functionality to scale on a pre-defined number of workers

## Requirements

In order to use this utility, it is recommended to enable [Trino fault-tolerant execution](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/trino-ft.html) to avoid query failures during resize operations.

Also, in order to perform scaling operations and create Instance Groups / Fleets, the utility requires that the EC2 Instance Role of the node where the application is running provides the following IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "autoscaling",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:AddInstanceFleet",
        "elasticmapreduce:ListInstanceFleets",
        "elasticmapreduce:ModifyInstanceFleet",
        "elasticmapreduce:AddInstanceGroups",
        "elasticmapreduce:ListInstanceGroups",
        "elasticmapreduce:ModifyInstanceGroups"
      ],
      "Resource": "*"
    }
  ]
}
```

## Setup

### Build & Testing
To use the application you should build the application using [scala sbt](https://www.scala-sbt.org/), and then
you launch the application using the main class `com.amazonaws.emr.TrinoAutoscaler`. Below a sample command:

```bash
# build and run
sbt clean run
```

### Build from an EC2 instance

To build the project and store the artifacts on Amazon S3, launch the following snippet from an Amazon Linux 2 instance. 
Make sure the EC2 instance Role has S3 permissions to store the artifacts in the bucket.

In the script below replace MY_BUCKET_NAME with the S3 bucket name you want to use:

```bash
# run as root
sudo -s 
# install requirements
curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
mv sbt-rpm.repo /etc/yum.repos.d/
yum install -y git sbt java-11-amazon-corretto
# clone repo
git clone https://github.com/aws-samples/emr-trino-autoscale.git && cd emr-trino-autoscale
sh scripts/setup-artifact-bucket.sh MY_BUCKET_NAME
```

### Install on Amazon EMR

Once built, you can launch an EMR cluster (>= 6.9.0 with Trino installed) adding as EMR STEP the
script [emr-install_autoscale.sh](./scripts/emr-install_autoscale.sh) to install the utility on the EMR master node.
The script will install the Trino autoscale as system daemon, so that it is relaunched in case of failures or unexpected
termination. The script requires to specify as parameter the bucket name used to store the artifacts buckets. For more
details check the header of the script. 

### CloudFormation Templates

The [templates](./templates) folder contains sample Cloudformation templates to launch EMR clusters using the autoscaling. 
The templates will launch an EMR on EC2 cluster with Trino Fault-Tolerant execution enabled, and install the trino autoscaler.

In order to use the templates, build the project from an Amazon Linux 2 Instance and deploy it on an Amazon S3 bucket. 

```bash
sh setup-artifact-bucket.sh MY_BUCKET_NAME
```

Once deployed, launch the template using the CloudFormation console. 
You can launch an [Instance Group Cluster](./templates/trino-cluster-ig.yml) or an [Instance Fleet Cluster](./templates/trino-cluster-if.yml). 
While launching the template, specify the following required parameters: 

- **Artifacts** The bucket name used in the script *setup-artifact-bucket.sh* e.g. MY_BUCKET_NAME
- **Subnet** The subnet where the EMR cluster is launched
- **SSH Key Name** EC2 SSH access key to access the cluster
- **Exchange Path** Amazon S3 buckets used for spooling Trino data (required for fault-tolerant execution)

## Resources

- [Documentation - EMR Trino](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/trino-ft.html)
- [Documentation - Trino Official](https://trino.io/docs/current/admin/fault-tolerant-execution.html)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
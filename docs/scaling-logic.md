## Scaling Logic

The trino autoscaler performs a CPU based evaluation of the workers registered in the cluster to determine how to scale the TASK fleet. 
As EMR best practice, the application perform the scaling of the cluster by managing TASK fleets or instance groups as these nodes can be more quickly provisioned compared to CORE nodes. For this reason scaling actions are only performed on this specific sets of instances.

The application provision the TASK fleet and groups based on the configurations defined in the [application.conf](../src/main/resources/application.conf). 
It's also able to reuse existing Instance Fleets or Groups if they match the same specifications (instance type, market, name). 

It's not required to create any TASK fleet or group to use the autoscaler, but this might still be useful if you want to specify custom configurations for the TASK fleet (e.g. increase task.concurrency on instances with a high number of CPU).

### Algorithm

The evaluation is performed by checking a fraction of the nodes of the cluster to verify if they breached the CPU threshold for a scaling operation. 

The scaling operations are performed in the following way:
- **Expand** The cluster is expanded if the node fraction defined (80% of the nodes) had an average CPU utilization greater than 80% in the last minute.
- **Shrink** The cluster is shrinked if the node fraction defined (80% of the nodes) had an average CPU utilization lower than 40% in the last minute.
- **No Action** If the current fleet has a CPU utilization between 40% and 80%, no scaling action will be performed.

#### Instance Groups
Instance Groups are enabled by default to use a concurrent scaling feature. This allows to scale at the same time multiple groups when a scaling action is received. 
This functionality helps in situations where you might be hitting ICE errors for some specific instance types defined in the autoscaler. 

If a specific group end up with an error during the scaling, the instance groups for that specific instance is healed (restoring the capacity to the current running count) and then it is excluded for expand operation for a pre-defined period of time (10 minutes). This mitigates chances to face again the same issue when a new scaling action is performed.
When the time expires the IG can be used again for autoscaling purposes.

#### Instance Fleets
The autoscaling mainly relies on Instance Fleets built-in features, so it only provides the desired count of units during scaling operations. 
In terms of best practice, we recommend that you specify an [Allocation Strategy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html) when launching an Instance Fleet cluster.

### Monitoring
The scaling is performed by constantly monitoring metrics exposed by each node on the Trino REST API `/v1/jmx/mbean`, and retrieving the mbeans metrics of interest.
This monitoring option was preferred to the default trino jmx connector as long-running queries, or clusters with a high number of concurrent queries, might delay the metrics collection.

Currently, the monitor process retrieves the metrics of interest every 15 seconds to have at least 4 datapoints in a single minute for the scaling evaluation. 

### Notes
Amazon EMR limits shrink operations for recently added instances. If an instance is added to the cluster, it cannot be terminated before 10 minutes. 

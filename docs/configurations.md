## Configurations
The application settings can be found in the file [application.conf](../src/main/resources/application.conf). 
Modify the configurations in this file to match your needs. 

The following provides details about the configurations that you can use to control the autoscaler behaviour:

### EMR parameters
- **cluster.id** (default: empty) EMR cluster id used to invoke scaling actions using AWS SDK. If, empty, the application will retry the information from the master (if running on the EMR master). You should specify this parameter only if using the application outside the EMR master

### Amazon CloudWatch
- **cw.publish** (default: false) Publish metrics to Amazon CloudWatch
- **cw.dimension** (default: "JobFlowId") CW dimension to publish metrics
- **cw.namespace** (default: "AWS/EMR") CW namespace to publish metrics. When using the application outside the EMR master, you should use a different namespace than "AWS/EMR"

### Trino
- **trino.user** (default: hadoop) Default Trino user used by autoscaler
- **trino.password** (default: empty) Default Trino user password

### Instance Groups - Scaling configurations
- **scaling.ig.nodes.min** Minimum number of nodes provisioned for TASK Instance Groups
- **scaling.ig.nodes.max** Maximum number of nodes provisioned for TASK Instance Groups
- **scaling.ig.step.shrink** Scaling Step for shrink operations. Recommended to set this lower than the expand step
- **scaling.ig.step.expand** Scaling Step for expand operations
- **scaling.ig.instance.types** List of supported Instance Types used by autoscaling. You can specify up to 48 different instances. Make sure you don't have additional TASK on the cluster if you want to specify all 48 available Instance Groups
- **scaling.ig.useSpot** Boolean to specify if using SPOT instances for scaled instances
- **scaling.ig.concurrently** Boolean to enable concurrent scaling of multiple Instance Groups at the same time. For example if you have 3 IG and the autoscaling requests 10 instances, the instances will be spread across the three groups: (4, 3, 3). If disabled, the first instance group managed will be used

### Instance Fleets - Scaling configurations
- **scaling.if.units.min**  Minimum number of units provisioned for TASK Instance Fleets
- **scaling.if.units.max** Maximum number of units provisioned for TASK Instance Fleets
- **scaling.if.step.shrink** Scaling Step in units count for shrink operations
- **scaling.if.step.expand** Scaling Step in units count for expand operations
- **scaling.if.instance.types** List of supported Instance Types used by autoscaling
- **scaling.if.instance.units** List of weights defined for the Instance Types used by autoscaling. The list count should match the count in the `scaling.if.instance.types` parameter
- **scaling.if.useSpot** Boolean to specify if using SPOT instances for scaled instances

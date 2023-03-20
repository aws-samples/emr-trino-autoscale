## Auto-Scaling Hints
The auto-scaling integrates with the Trino required workers configurations, that allows you to specify a required number of workers before starting a specific query.
Trino allows to specify these configuration as cluster parameters or individual session parameters:

* (cluster) **query-manager.required-workers** - min number of workers before starting a query
* (cluster) **query-manager.required-workers-max-wait** - max time to wait for required workers before failing the query
* (SESSION) **required_workers_count** - equivalent SESSION parameter for **query-manager.required-workers**
* (SESSION) **required_workers_max_wait_time** - Equivalent SESSION parameter for **query-manager.required-workers-max-wait**. This configuration should be specified as Cluster parameter rather than SESSION

To use this functionality you can specify the required number of workers and wait time as SESSION parameters. For example:

```sql
SET SESSION required_workers_count=10; 
SET SESSION required_workers_max_wait_time='6m'; 
SELECT * FROM ....
```

If the cluster doesn't have the specified number of workers, the autoscaler will automatically provision the missing capacity. 

Below an example EMR configuration for setting the trino configurations as cluster parameters. However, this functionality should mainly be used with SESSION parameters.

```json
[
  {
    "Classification": "trino-config",
    "Properties": {
      "query-manager.required-workers": "10",
      "query-manager.required-workers-max-wait": "6m"
    }
  }
]
```


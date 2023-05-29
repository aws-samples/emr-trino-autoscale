package com.amazonaws.emr

import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest
import com.typesafe.config.ConfigFactory
import org.json4s.native.JsonMethods._

import java.io.File
import java.net.InetAddress
import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.io.Source
import scala.language.postfixOps
import scala.util.Try

object Config {

  private val config = ConfigFactory.load()
  private val trinoConf = ConfigFactory.parseFile(new File("/etc/trino/conf/config.properties"))
  private val JobFlowJson = "/emr/instance-controller/lib/info/job-flow.json"

  // =======================================================================
  // Amazon EMR
  // =======================================================================

  // Retrieve Cluster ID from the EMR master if not defined
  private val clusterID = config.getString("cluster.id")
  val EmrClusterId: String = if (isEmrMaster) getStringFromJson(JobFlowJson) else clusterID

  // Retrieve Master Public IP using SDK if cluster ID is defined (mainly for testing)
  // Otherwise we return the FQDN of the host
  val EmrClusterPrimary: String = if (isNotEmrMaster) {
    val retryPolicy = RetryPolicy.builder().withMaxErrorRetry(Config.AwsSdkMaxRetry).build()
    val clientConf = new ClientConfiguration().withRetryPolicy(retryPolicy)
    val client = AmazonElasticMapReduceClientBuilder.standard().withClientConfiguration(clientConf).build
    val request = new DescribeClusterRequest().withClusterId(clusterID)
    val response = client.describeCluster(request)
    response.getCluster.getMasterPublicDnsName
  } else InetAddress.getLocalHost.getCanonicalHostName

  val EmrAutoIdleTerminationFile = "/emr/metricscollector/isbusy"

  // =======================================================================
  // Amazon CloudWatch
  // =======================================================================
  val PublishMetrics: Boolean = config.getBoolean("cw.publish")
  val CloudWatchDimension: String = config.getString("cw.dimension")
  val CloudWatchNamespace: String = config.getString("cw.namespace")

  // =======================================================================
  // Trino
  // =======================================================================
  val TrinoUser: String = config.getString("trino.user")
  val TrinoPassword: String = config.getString("trino.password")

  // check if is running on http or https
  val isTrinoHttps = Try(trinoConf.getBoolean("http-server.https.enabled")).getOrElse(false)
  val TrinoRestSchema = if (isTrinoHttps) "https" else "http"
  val TrinoServerPort: String = if (isTrinoHttps) trinoConf.getString("http-server.https.port") else trinoConf.getString("http-server.http.port")
  val TrinoCoordinatorUrl = s"$TrinoRestSchema://$EmrClusterPrimary:$TrinoServerPort"
  val TrinoJmxImpl: String = "emr"

  // =======================================================================
  // Instance Groups - Scaling configurations
  // =======================================================================
  val IgMinNumNodes: Int = config.getInt("scaling.ig.nodes.min")
  val IgMaxNumNodes: Int = config.getInt("scaling.ig.nodes.max")
  val IgScaleConcurrently: Boolean = config.getBoolean("scaling.ig.concurrently")
  val IgScaleStepExpand: Int = config.getInt("scaling.ig.step.expand")
  val IgScaleStepShrink: Int = config.getInt("scaling.ig.step.shrink")
  val IgInstanceTypes: List[String] = config.getStringList("scaling.ig.instance.types").asScala.toList.distinct
  val IgShouldUseSpot: Boolean = config.getBoolean("scaling.ig.useSpot")

  // =======================================================================
  // Instance Fleets - Scaling configurations
  // =======================================================================
  val IfMinNumUnits: Int = config.getInt("scaling.if.units.min")
  val IfMaxNumUnits: Int = config.getInt("scaling.if.units.max")
  val IfScaleStepExpand: Int = config.getInt("scaling.if.step.expand")
  val IfScaleStepShrink: Int = config.getInt("scaling.if.step.shrink")
  val IfInstanceTypes: List[String] = config.getStringList("scaling.if.instance.types").asScala.toList
  val IfInstanceTypesUnits: List[String] = config.getStringList("scaling.if.instance.units").asScala.toList
  val IfShouldUseSpot: Boolean = config.getBoolean("scaling.if.useSpot")

  // =======================================================================
  // Scaling configurations - DO NOT MODIFY
  // =======================================================================
  val AwsSdkMaxRetry = 10
  val WorkersNamePrefix = "AutoScaling-TASK"

  // Service Limit - we can have max 48 Task Groups in IG cluster
  val MaxInstanceGroupsLimit = 48

  // MetricsMaxDataPoints(20) * MetricsCollectInterval(15s) = 5 minutes metrics
  // Define the max number of data points to keep in memory (Default: 20)
  val MetricsMaxDataPoints = 20
  val MetricsDataPointsOneMin = 4
  // Metric collection interval (Default: 15 sec)
  val MetricsCollectInterval: FiniteDuration = 15 seconds
  // Scaling evaluation interval (Default: 15 sec)
  val MetricsEvaluationInterval: FiniteDuration = 15 seconds

  private def getStringFromJson(path: String): String = {
    val data = parse(Source.fromFile(path).getLines.mkString)
    (data \ "jobFlowId").values.toString
  }

  def isEmrMaster: Boolean = {
    if (clusterID.isEmpty) true
    else false
  }

  def isNotEmrMaster: Boolean = !isEmrMaster

}
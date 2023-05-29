package com.amazonaws.emr.metrics

import akka.actor.ActorSystem
import akka.http.javadsl.model.headers.Authorization.basic
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.amazonaws.ClientConfiguration
import com.amazonaws.emr.Config
import com.amazonaws.emr.Config._
import com.amazonaws.emr.metrics.models._
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest
import org.apache.logging.log4j.scala.Logging
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class TrinoJmxRest(implicit val system: ActorSystem) extends TrinoJmx with Logging {

  private val JmxPath = "v1/jmx/mbean"
  private val QueryTimeout = 5.seconds

  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  def getClusterMemory: Future[ClusterMemoryMetrics] = {
    getMBeanAttributes(s"${Config.TrinoCoordinatorUrl}/$JmxPath/trino.memory:name=ClusterMemoryManager").map(m =>
      ClusterMemoryMetrics(
        clusterMemoryBytes = m.getOrElse("ClusterMemoryBytes", 0).asInstanceOf[BigInt],
        clusterTotalMemoryReservation = m.getOrElse("ClusterTotalMemoryReservation", 0).asInstanceOf[BigInt],
        clusterUserMemoryReservation = m.getOrElse("ClusterUserMemoryReservation", 0).asInstanceOf[BigInt],
        numberFleakedQueries = m.getOrElse("NumberOfLeakedQueries", 0).asInstanceOf[BigInt],
        queriesKilledDueToOutOfMemory = m.getOrElse("QueriesKilledDueToOutOfMemory", 0).asInstanceOf[BigInt],
        tasksKilledDueToOutOfMemory = m.getOrElse("TasksKilledDueToOutOfMemory", 0).asInstanceOf[BigInt]
      ))
  }

  def getClusterQueryStats: Future[ClusterQueriesMetrics] = {
    getMBeanAttributes(s"${Config.TrinoCoordinatorUrl}/$JmxPath/trino.execution:name=QueryManager").map { m =>
      ClusterQueriesMetrics(
        oneMinAbandoned = m.getOrElse("AbandonedQueries.OneMinute.Count", 0).asInstanceOf[Double],
        oneMinCancelled = m.getOrElse("CanceledQueries.OneMinute.Count", 0).asInstanceOf[Double],
        oneMinCompleted = m.getOrElse("CompletedQueries.OneMinute.Count", 0).asInstanceOf[Double],
        oneMinFailed = m.getOrElse("FailedQueries.OneMinute.Count", 0).asInstanceOf[Double],
        oneMinSubmitted = m.getOrElse("SubmittedQueries.OneMinute.Count", 0).asInstanceOf[Double],
        fiveMinAbandoned = m.getOrElse("AbandonedQueries.FiveMinute.Count", 0).asInstanceOf[Double],
        fiveMinCancelled = m.getOrElse("CanceledQueries.FiveMinute.Count", 0).asInstanceOf[Double],
        fiveMinCompleted = m.getOrElse("CompletedQueries.FiveMinute.Count", 0).asInstanceOf[Double],
        fiveMinFailed = m.getOrElse("FailedQueries.FiveMinute.Count", 0).asInstanceOf[Double],
        fiveMinSubmitted = m.getOrElse("SubmittedQueries.FiveMinute.Count", 0).asInstanceOf[Double],
        queuedQueries = m.getOrElse("QueuedQueries", 0).asInstanceOf[BigInt].toInt,
        runningQueries = m.getOrElse("RunningQueries", 0).asInstanceOf[BigInt].toInt
      )
    }
  }

  def getClusterNodesCpuStats: Future[List[Option[ClusterNodesCpuMetrics]]] = {

    val internalIp = Await.result(getClusterNodes, QueryTimeout).map { n =>
      val domainPattern = raw".*//(.*):.*".r
      n.uri match {
        case domainPattern(localPart) => localPart
        case _ => ""
      }
    }.filter(_.nonEmpty)

    /* START - Autoscaling using Public Dns Hostnames */
    val activeNodes = if (Config.isNotEmrMaster) {
      val retryPolicy = RetryPolicy.builder().withMaxErrorRetry(Config.AwsSdkMaxRetry).build()
      val clientConf = new ClientConfiguration().withRetryPolicy(retryPolicy)
      val client = AmazonElasticMapReduceClientBuilder.standard().withClientConfiguration(clientConf).build

      val request = new ListInstancesRequest().withClusterId(EmrClusterId)
      client.listInstances(request)
        .getInstances.asScala.toList
        .filter(_.getStatus.getState.equalsIgnoreCase("RUNNING"))
        .filter(i => internalIp.contains(i.getPrivateIpAddress))
        .map(_.getPublicDnsName)
    } else internalIp
    /* END - Autoscaling using Public Dns Hostnames */


    val nodeStats = activeNodes.map { node =>
      val nodeUrl = s"$TrinoRestSchema://$node:$TrinoServerPort/$JmxPath/java.lang:type=OperatingSystem"
      getMBeanAttributes(nodeUrl).map { m =>
        //logger.debug(s"node:$node map: $m")
        if (m.isEmpty) None
        else Some(ClusterNodesCpuMetrics(
          node,
          availableProcessors = m.getOrElse("AvailableProcessors", 0.0).asInstanceOf[BigInt].toInt,
          cpuLoad = m.getOrElse("CpuLoad", 0).asInstanceOf[Double],
          processCpuLoad = m.getOrElse("ProcessCpuLoad", 0).asInstanceOf[Double],
          systemCpuLoad = m.getOrElse("SystemCpuLoad", 0).asInstanceOf[Double],
          systemLoadAverage = m.getOrElse("SystemLoadAverage", 0).asInstanceOf[Double],
        ))
      }
    }
    Future.sequence(nodeStats)
  }

  def getRequiredWorkers: Future[ClusterSizeMonitor] = {
    getMBeanAttributes(s"${Config.TrinoCoordinatorUrl}/$JmxPath/trino.execution:name=ClusterSizeMonitor").map(m =>
      ClusterSizeMonitor(requiredWorkers = m.getOrElse("RequiredWorkers", 0).asInstanceOf[BigInt].toInt))
  }

  private def getClusterNodes: Future[List[NodeStatus]] = {
    val request = HttpRequest(uri = s"${Config.TrinoCoordinatorUrl}/v1/node")
      .withHeaders(basic(TrinoUser, TrinoPassword))
    Http().singleRequest(request).flatMap { response =>
      Unmarshal(response).to[String].map { data =>
        val json = JsonMethods.parse(data)
        json.extract[List[NodeStatus]]
      }
    }
  }

  private def getMBeanAttributes(url: String): Future[Map[String, Any]] = {
    val request = HttpRequest(uri = url).withHeaders(basic(TrinoUser, TrinoPassword))
    Http().singleRequest(request).flatMap { response =>
      Unmarshal(response).to[String].map { data =>
        val parsed = JsonMethods.parse(data)
        val res = (parsed \ "attributes").extract[List[Attribute]]
        res.map(c => c.name -> c.value).toMap
      }
    }.fallbackTo(Future.successful(Map.empty[String, Any]))
  }

}

package com.amazonaws.emr.metrics

import akka.actor.ActorSystem
import akka.http.javadsl.model.headers.Authorization.basic
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.amazonaws.emr.Config
import com.amazonaws.emr.Config._
import com.amazonaws.emr.metrics.models._
import org.apache.logging.log4j.scala.Logging
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

import javax.management.{MBeanServerConnection, ObjectName}
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class TrinoJmxEmr(implicit val system: ActorSystem) extends TrinoJmx with Logging {

  private val JmxPath = "v1/jmx/mbean"
  private val QueryTimeout = 5.seconds

  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  // Run on coordinator
  private val localUrl = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://localhost:9080/jmxrmi")
  private val jmxc = JMXConnectorFactory.connect(localUrl)
  private val mBean = jmxc.getMBeanServerConnection

  private def getAttribute(objName: String, attrName: String) = getRemoteAttribute(mBean, objName, attrName)

  private def getRemoteAttribute(srv: MBeanServerConnection, objName: String, attrName: String) = {
    srv.getAttribute(new ObjectName(objName), attrName)
  }

  def getClusterMemory: Future[ClusterMemoryMetrics] = {

    val objName = "trino.memory:name=ClusterMemoryManager"
    Future {
      ClusterMemoryMetrics(
        clusterMemoryBytes = getAttribute(objName, "ClusterMemoryBytes").asInstanceOf[Long],
        clusterTotalMemoryReservation = getAttribute(objName, "ClusterTotalMemoryReservation").asInstanceOf[Long],
        clusterUserMemoryReservation = getAttribute(objName, "ClusterUserMemoryReservation").asInstanceOf[Long],
        numberFleakedQueries = getAttribute(objName, "NumberOfLeakedQueries").asInstanceOf[Int],
        queriesKilledDueToOutOfMemory = getAttribute(objName, "QueriesKilledDueToOutOfMemory").asInstanceOf[Long],
        tasksKilledDueToOutOfMemory = getAttribute(objName, "TasksKilledDueToOutOfMemory").asInstanceOf[Long]
      )
    }
  }

  def getClusterQueryStats: Future[ClusterQueriesMetrics] = {
    val objName = "trino.execution:name=QueryManager"
    Future {
      ClusterQueriesMetrics(
        oneMinAbandoned = getAttribute(objName, "AbandonedQueries.OneMinute.Count").asInstanceOf[Double],
        oneMinCancelled = getAttribute(objName, "CanceledQueries.OneMinute.Count").asInstanceOf[Double],
        oneMinCompleted = getAttribute(objName, "CompletedQueries.OneMinute.Count").asInstanceOf[Double],
        oneMinFailed = getAttribute(objName, "FailedQueries.OneMinute.Count").asInstanceOf[Double],
        oneMinSubmitted = getAttribute(objName, "SubmittedQueries.OneMinute.Count").asInstanceOf[Double],
        fiveMinAbandoned = getAttribute(objName, "AbandonedQueries.FiveMinute.Count").asInstanceOf[Double],
        fiveMinCancelled = getAttribute(objName, "CanceledQueries.FiveMinute.Count").asInstanceOf[Double],
        fiveMinCompleted = getAttribute(objName, "CompletedQueries.FiveMinute.Count").asInstanceOf[Double],
        fiveMinFailed = getAttribute(objName, "FailedQueries.FiveMinute.Count").asInstanceOf[Double],
        fiveMinSubmitted = getAttribute(objName, "SubmittedQueries.FiveMinute.Count").asInstanceOf[Double],
        queuedQueries = getAttribute(objName, "QueuedQueries").asInstanceOf[Long].toInt,
        runningQueries = getAttribute(objName, "RunningQueries").asInstanceOf[Long].toInt
      )
    }
  }

  def getClusterNodesCpuStats: Future[List[Option[ClusterNodesCpuMetrics]]] = {

    val activeNodes = Await.result(getClusterNodes, QueryTimeout)
      .filter(_.nodeRole.equalsIgnoreCase("worker"))
      .filter(_.nodeState.equalsIgnoreCase("active"))
      .map { n =>
      val domainPattern = raw".*//(.*):.*".r
      n.nodeURI match {
        case domainPattern(localPart) => localPart
        case _ => ""
      }
    }.filter(_.nonEmpty)

    val nodeStats = activeNodes.map { node =>
      val nodeUrl = s"$TrinoRestSchema://$node:$TrinoServerPort/$JmxPath/java.lang:type=OperatingSystem"
      getMBeanAttributes(nodeUrl).map { m =>
        logger.debug(s"node:$node map: $m")
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
    val objName = "trino.execution:name=ClusterSizeMonitor"
    Future {
      ClusterSizeMonitor(
        requiredWorkers = getAttribute(objName, "RequiredWorkers").asInstanceOf[Int]
      )
    }
  }

  private def getClusterNodes: Future[List[EmrNodeStatus]] = {
    val request = HttpRequest(uri = s"${Config.TrinoCoordinatorUrl}/v1/autoscale")
      .withHeaders(basic(TrinoUser, TrinoPassword))
    Http().singleRequest(request).flatMap { response =>
      Unmarshal(response).to[String].map { data =>
        val json = JsonMethods.parse(data)
        json.extract[List[EmrNodeStatus]]
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

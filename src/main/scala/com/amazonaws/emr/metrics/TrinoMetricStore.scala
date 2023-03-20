package com.amazonaws.emr.metrics

import akka.actor.ActorSystem
import com.amazonaws.emr.Config.PublishMetrics
import com.amazonaws.emr.metrics.models._
import com.amazonaws.emr.utils.CloudWatch
import org.apache.logging.log4j.scala.Logging
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

class TrinoMetricStore()(implicit val system: ActorSystem) extends Logging {

  implicit val executor: ExecutionContextExecutor = system.dispatcher

  val nodesCpuStats = new MetricStore

  private var clusterMemory: ClusterMemoryMetrics = ClusterMemoryMetrics(0, 0, 0, 0, 0, 0)
  private var clusterQueries: ClusterQueriesMetrics = ClusterQueriesMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  private var clusterSizeMonitor: ClusterSizeMonitor = ClusterSizeMonitor(0)
  private var clusterNodesCpuStats: List[ClusterNodesCpuMetrics] = List()
  private var runningQueriesStats: List[TrinoQuery] = List()

  private val trinoJmx = new TrinoJmxApiRest

  implicit val formats: DefaultFormats.type = DefaultFormats

  /** Collect all Trino metrics */
  def collect(): Unit = {

    val result = for {
      clusterMemory <- trinoJmx.getClusterMemory
      clusterQueryStats <- trinoJmx.getClusterQueryStats
      clusterRequired <- trinoJmx.getRequiredWorkers
      runningQueries <- trinoJmx.getQueriesRunning
      cpuStats <- trinoJmx.getClusterNodesCpuStats
    } yield (clusterMemory, clusterQueryStats, clusterRequired, runningQueries, cpuStats)

    result.onComplete {
      case Success(x) =>
        clusterMemory = x._1
        logger.info(s"Cluster memory stats: $clusterMemory")
        clusterQueries = x._2
        logger.info(s"Cluster queries stats: $clusterQueries")
        clusterSizeMonitor = x._3
        logger.info(s"Cluster required workers: $getRequiredWorkers")

        runningQueriesStats = x._4
        runningQueriesStats.foreach(q => logger.debug(Json(DefaultFormats).write(q)))

        clusterNodesCpuStats = x._5.filter(_.nonEmpty).map(_.get)
        nodesCpuStats.append(clusterNodesCpuStats.map(e => e.node -> e.processCpuLoad).toMap)
        logger.debug(s"Nodes CPU metrics - 1 min")
        nodesCpuStats.oneMinute.keys.foreach(m => logger.debug(s"$m: ${nodesCpuStats.oneMinute.getOrElse(m, List()).mkString("", ", ", "")} "))
        logger.info(s"Nodes CPU metrics - 1 min avg (cold: ${nodesCpuStats.isCold})")
        nodesCpuStats.oneMinuteAvg.foreach(m => logger.info(s"${m._1}: ${m._2}"))

        if (PublishMetrics) {
          try {
            CloudWatch.publish("trino.cpu", nodesCpuStats.last)
            CloudWatch.publish("trino.requiredWorkers", getRequiredWorkers)
            CloudWatch.publish("trino.totalWorkers", clusterNodesCpuStats.size)
            CloudWatch.publish("trino.totalAvailableCores", getAvailableCores)
            CloudWatch.publish("trino.totalOneMinFailedQueries", getTotalOneMinFailedQueries)
            CloudWatch.publish("trino.totalQueuedQueries", getTotalQueuedQueries)
            CloudWatch.publish("trino.totalRunningQueries", getTotalRunningQueries)
            CloudWatch.publish("trino.totalQueuedDrivers", getTotalQueuedDrivers)
            CloudWatch.publish("trino.totalRunningDrivers", getTotalRunningDrivers)
          } catch {
            case e: Exception =>
              logger.error("Failed to send metrics to CloudWatch")
              e.printStackTrace()
          }
        }

      case Failure(e) =>
        println("Error collecting Trino metrics")
        e.printStackTrace()

    }
  }

  /** Return the required number of workers to start a query */
  def getRequiredWorkers: Int = clusterSizeMonitor.requiredWorkers

  /** Return the number of available processors in the cluster (workers only) */
  def getAvailableCores: Int = clusterNodesCpuStats.map(_.availableProcessors).sum

  /** Retrieve total number of queued drivers */
  def getTotalQueuedDrivers: Int = runningQueriesStats.map(_.queryStats.queuedDrivers).sum

  /** Retrieve total number of running drivers */
  def getTotalRunningDrivers: Int = runningQueriesStats.map(_.queryStats.runningDrivers).sum

  /** Return the total number of failed queries in 1 minute */
  def getTotalOneMinFailedQueries: Double = clusterQueries.oneMinFailed

  /** Return the total number of queued queries */
  def getTotalQueuedQueries: Int = clusterQueries.queuedQueries

  /** Return the number of running queries */
  def getTotalRunningQueries: Int = runningQueriesStats.size

  def getQueryStats: ClusterQueriesMetrics = clusterQueries
}

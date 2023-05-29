package com.amazonaws.emr.metrics

import com.amazonaws.emr.TrinoAutoscaler.system
import com.amazonaws.emr.metrics.models._

import scala.concurrent.Future

trait TrinoJmx {

  /** Retrieve Cluster Memory Stats */
  def getClusterMemory: Future[ClusterMemoryMetrics]

  /** Retrieve Cluster Queries Stats */
  def getClusterQueryStats: Future[ClusterQueriesMetrics]

  /** Retrieve Node Cpu Stats */
  def getClusterNodesCpuStats: Future[List[Option[ClusterNodesCpuMetrics]]]

  /** Retrieve the number of Trino required_workers */
  def getRequiredWorkers: Future[ClusterSizeMonitor]

}

object TrinoJmx {
  def apply(name: String): TrinoJmx = {
    name.toLowerCase() match {
      case "emr" => new TrinoJmxEmr()
      case "rest" => new TrinoJmxRest()
      case _ =>
        throw new IllegalArgumentException("Invalid trino.jmx.impl configuration. Allowed values [emr, rest]")
    }
  }
}
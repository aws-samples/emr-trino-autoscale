package com.amazonaws.emr.metrics

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

  /** Retrieve statistics for any running query on the cluster */
  def getQueriesRunning: Future[List[TrinoQuery]]

}

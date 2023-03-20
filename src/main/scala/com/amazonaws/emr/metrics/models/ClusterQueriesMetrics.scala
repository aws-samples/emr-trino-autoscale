package com.amazonaws.emr.metrics.models

case class ClusterQueriesMetrics(
  oneMinAbandoned: Double,
  oneMinCancelled: Double,
  oneMinCompleted: Double,
  oneMinFailed: Double,
  oneMinSubmitted: Double,
  fiveMinAbandoned: Double,
  fiveMinCancelled: Double,
  fiveMinCompleted: Double,
  fiveMinFailed: Double,
  fiveMinSubmitted: Double,
  queuedQueries: Int,
  runningQueries: Int) {
  override def toString: String =
    s"""running: $runningQueries queued: $queuedQueries
       | 1m abandoned: $oneMinAbandoned cancelled: $oneMinCancelled completed: $oneMinCompleted failed: $oneMinFailed submitted: $oneMinSubmitted
       | 5m abandoned: $fiveMinAbandoned cancelled: $fiveMinCancelled completed: $fiveMinCompleted failed: $fiveMinFailed submitted: $fiveMinSubmitted""".stripMargin
}

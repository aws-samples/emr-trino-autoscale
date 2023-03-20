package com.amazonaws.emr.metrics.models

case class ClusterMemoryMetrics(
  clusterMemoryBytes: BigInt,
  clusterTotalMemoryReservation: BigInt,
  clusterUserMemoryReservation: BigInt,
  numberFleakedQueries: BigInt,
  queriesKilledDueToOutOfMemory: BigInt,
  tasksKilledDueToOutOfMemory: BigInt) {
  override def toString: String =
    s"(bytes) memory: $clusterMemoryBytes reserved: $clusterTotalMemoryReservation user: $clusterUserMemoryReservation"
}
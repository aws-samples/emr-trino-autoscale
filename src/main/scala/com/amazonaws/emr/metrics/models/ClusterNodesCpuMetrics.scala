package com.amazonaws.emr.metrics.models

case class ClusterNodesCpuMetrics(
  node: String,
  availableProcessors: Int,
  cpuLoad: Double,
  processCpuLoad: Double,
  systemCpuLoad: Double,
  systemLoadAverage: Double)
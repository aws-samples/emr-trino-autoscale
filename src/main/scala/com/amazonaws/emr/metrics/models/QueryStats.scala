package com.amazonaws.emr.metrics.models

case class QueryStats(
  createTime: String,
  queuedTime: String,
  elapsedTime: String,
  executionTime: String,
  failedTasks: Int,
  totalDrivers: Int,
  queuedDrivers: Int,
  runningDrivers: Int,
  completedDrivers: Int,
  rawInputDataSize: String,
  rawInputPositions: Int,
  fullyBlocked: Boolean,
  blockedReasons: List[String])

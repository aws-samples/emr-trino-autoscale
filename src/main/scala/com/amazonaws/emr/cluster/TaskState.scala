package com.amazonaws.emr.cluster

case class TaskState(
  id: String,
  status: String,
  isResizing: Boolean,
  isSuspended: Boolean,
  isExcluded: Boolean,
  running: Int,
  requested: Int,
  message: String,
  lastSuspendedTimeMs: Long)

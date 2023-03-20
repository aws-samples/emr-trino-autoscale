package com.amazonaws.emr.metrics.models

case class TrinoQuery(
  queryId: String,
  state: String,
  self: String,
  resourceGroupId: List[String],
  scheduled: Boolean,
  queryStats: QueryStats)

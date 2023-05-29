package com.amazonaws.emr.metrics.models

case class EmrNodeStatus
(
  nodeId: String,
  nodeVersion: String,
  nodeURI: String,
  nodeRole: String,
  nodeState: String
)

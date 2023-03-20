package com.amazonaws.emr.metrics.models

/**
 * Provides the status of the nodes from the following API `/v1/node` on the coordinator
 * Example Response:
 * {
 * "uri": "http://172.31.1.47:8889/v1/status",
 * "recentRequests": 119.00277776491771,
 * "recentFailures": 0,
 * "recentSuccesses": 119.00277776491771,
 * "lastRequestTime": "2023-03-03T15:32:01.895Z",
 * "lastResponseTime": "2023-03-03T15:32:01.895Z",
 * "recentFailureRatio": 0,
 * "age": "49.06m",
 * "recentFailuresByType": {}
 * }
 */
case class NodeStatus(
  uri: String,
  recentRequests: Double,
  recentFailures: Double,
  recentSuccesses: Double,
  lastRequestTime: String,
  lastResponseTime: String,
  recentFailureRatio: Double,
  age: String)


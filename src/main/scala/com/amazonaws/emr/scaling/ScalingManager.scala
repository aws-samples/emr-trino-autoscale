package com.amazonaws.emr.scaling

import com.amazonaws.emr.Config
import com.amazonaws.emr.cluster.Workers
import com.amazonaws.emr.metrics.TrinoMetricStore
import com.amazonaws.emr.metrics.models.ClusterQueriesMetrics
import com.amazonaws.emr.scaling.ResizeType.{EXPAND, SHRINK}
import com.amazonaws.emr.scaling.algo.{CpuLoadEvaluator, StepScalingAlgorithm}
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.scala.Logging

import java.io.File
import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ScalingManager(workers: Workers) extends Logging {

  // Time to wait after a completed resize operation
  // The cooldown give time to running or queued queries to be distributed across the cluster
  private val scalingCoolDownTimeMs: Long = 2.minutes.toMillis
  private val evaluator = new CpuLoadEvaluator
  private val algorithm = new StepScalingAlgorithm(evaluator, workers)
  private var scalingLastOpTimeMs: Long = 0L

  def evaluate(store: TrinoMetricStore): Unit = {

    if (isIdle(store.getQueryStats)) {
      logger.info("Trino is Idle. No query activity was performed in the last 5 minutes")
      evaluateAndPerform(ResizeAction(SHRINK, workers.minCapacity))
    } else if (workers.isResizing) {
      logger.info("Skip evaluation. Cluster is still resizing")
      scalingLastOpTimeMs = System.currentTimeMillis()
    } else if(store.getRequiredWorkers > workers.running) {
      logger.info("Required Workers are greater than current capacity")
      evaluateAndPerform(ResizeAction(EXPAND, store.getRequiredWorkers))
    } else {
      if ((System.currentTimeMillis() - scalingLastOpTimeMs) > scalingCoolDownTimeMs) {
        evaluateAndPerform(algorithm.evaluate(workers.running, store.getRequiredWorkers, store.nodesCpuStats))
      } else {
        logger.info(
          s"Skip evaluation. Last scaling action performed at ${
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(scalingLastOpTimeMs), ZoneId.of("UTC"))
          }")
      }
    }

    // Check if we are on the EMR master and if there are running queries
    // Performs a touch on the file monitored by the auto-termination feature
    if (Config.isEmrMaster && isNotIdle(store.getQueryStats)) {
      logger.info(s"Touch: ${Config.EmrAutoIdleTerminationFile} ")
      try {
        FileUtils.touch(new File(Config.EmrAutoIdleTerminationFile))
      } catch {
        case e: Throwable =>
          logger.error(s"Cannot touch ${Config.EmrAutoIdleTerminationFile}. Auto-Idle termination might not work")
          logger.warn(s"Make sure ${Config.EmrAutoIdleTerminationFile} is owned by the same user running the autoscale (default: trino)")
          e.printStackTrace()
      }
    }
  }

  /**
   * Evaluate if we need to request a scaling action based on the current status of the cluster.
   * There are multiple explicit conditions to avoid generating unuseful scaling requests to the service.
   */
  private def evaluateAndPerform(op: ResizeAction): Unit = {

    val currentCapacity = workers.running
    val requestedCapacity = workers.requested

    op.action match {

      case com.amazonaws.emr.scaling.ResizeType.EXPAND =>
        if (currentCapacity == workers.maxCapacity) {
          logger.info(s"No Scaling - already provisioned max capacity: ${workers.maxCapacity} ${workers.units}")
        } else if (currentCapacity > workers.maxCapacity) {
          // Safety - in case users manually resize the cluster above limits
          logger.info(s"Scaling - Shrink - above max capacity (max: ${workers.maxCapacity} ${workers.units})")
          logger.info(s"Scaling - Shrink - from $currentCapacity to ${op.capacity} ${workers.units}")
          workers.resize(workers.maxCapacity)
        } else {
          logger.info(s"Scaling - Expand - from $currentCapacity to ${op.capacity} ${workers.units}")
          workers.resize(op.capacity)
        }

      case com.amazonaws.emr.scaling.ResizeType.SHRINK =>
        if (currentCapacity == workers.minCapacity) {
          logger.info(s"No Scaling - already provisioned min capacity: ${workers.minCapacity} ${workers.units}")
        } else if (requestedCapacity == op.capacity) {
          logger.info(s"No Scaling - already requested the same capacity: $requestedCapacity ${workers.units}")
        } else {
          logger.info(s"Scaling - Shrink - from $currentCapacity to ${op.capacity} ${workers.units}")
          workers.resize(op.capacity)
        }

      case ResizeType.NONE =>
        logger.info(s"No Scaling - no action required")

    }
  }

  /**
   * We consider the cluster idle if in the last five minutes there was no activity.
   * This means that no query was running, queued, failed, abandoned, cancelled, completed or submitted.
   *
   * Trino metrics do not report a zero count, but rather a value below 1 for this counter.
   * This applies to both DDL and DML commands.
   */
  private def isIdle(metric: ClusterQueriesMetrics): Boolean = {
    metric.queuedQueries == 0 && metric.runningQueries == 0 &&
      metric.fiveMinFailed < 1 && metric.fiveMinAbandoned < 1 &&
      metric.fiveMinCancelled < 1 && metric.fiveMinCompleted < 1 &&
      metric.fiveMinSubmitted < 1
  }

  private def isNotIdle(metric: ClusterQueriesMetrics): Boolean = !isIdle(metric)

}
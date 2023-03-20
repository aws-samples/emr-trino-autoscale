package com.amazonaws.emr.scaling.algo

import com.amazonaws.emr.metrics.MetricStore
import com.amazonaws.emr.scaling.ResizeType
import com.amazonaws.emr.scaling.ResizeType.ResizeType
import org.apache.logging.log4j.scala.Logging

class CpuLoadEvaluator extends Evaluator with Logging {

  override val Label: String = "CPU Evaluation"

  private val nodesFraction = 0.8 // 80% of the Presto Workers (CORE + TASK)
  private val expandThreshold = 0.7 // 70% cpu utilization
  private val shrinkThreshold = 0.5 // 50% cpu utilization

  logger.info(s"CpuLoadEvaluator initialized with: nodesFraction=$nodesFraction expandThreshold=$expandThreshold shrinkThreshold=$shrinkThreshold")

  override def evaluate(metrics: MetricStore): ResizeType = {
    if (metrics.isCold) {
      logger.info(s"$Label - no action - Not enough data points collected to perform an evaluation")
      ResizeType.NONE
    } else if (metrics.isAvgOneMinGreater(nodesFraction, expandThreshold)) {
      logger.info(s"$Label - should expand - avg cpu on ${nodesFraction * 100}% nodes greater than ${expandThreshold * 100}%")
      ResizeType.EXPAND
    } else if (metrics.isAvgOneMinBetween(nodesFraction, shrinkThreshold, expandThreshold)) {
      logger.info(s"$Label - no action - avg cpu on ${nodesFraction * 100}% nodes > ${shrinkThreshold * 100}% && < ${expandThreshold * 100}%")
      ResizeType.NONE
    } else {
      logger.info(s"$Label - should shrink - avg cpu on ${nodesFraction * 100}% nodes < ${shrinkThreshold * 100}%")
      ResizeType.SHRINK
    }
  }

}

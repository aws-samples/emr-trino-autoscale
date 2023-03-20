package com.amazonaws.emr.scaling.algo

import com.amazonaws.emr.cluster.Workers
import com.amazonaws.emr.metrics.MetricStore
import com.amazonaws.emr.scaling.{ResizeAction, ResizeType}

trait ScalingAlgorithm {

  val evaluator: Evaluator
  val workers: Workers

  def evaluate(capacity: Int, required: Int, metrics: MetricStore): ResizeAction = {

    evaluator.evaluate(metrics) match {
      case ResizeType.EXPAND => ResizeAction(ResizeType.EXPAND, expand(capacity, required))
      case ResizeType.SHRINK => ResizeAction(ResizeType.SHRINK, shrink(capacity))
      case ResizeType.NONE =>
        if (required > 0 && required < capacity) ResizeAction(ResizeType.EXPAND, expand(capacity, required))
        else ResizeAction(ResizeType.NONE, capacity)
    }
  }

  /** Determine additional capacity to add in the cluster */
  def expand(running: Int, required: Int): Int

  /** Determine capacity to remove in the cluster */
  def shrink(running: Int): Int

}

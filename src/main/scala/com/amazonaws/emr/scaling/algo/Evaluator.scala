package com.amazonaws.emr.scaling.algo

import com.amazonaws.emr.metrics.MetricStore
import com.amazonaws.emr.scaling.ResizeType.ResizeType

trait Evaluator {

  val Label: String

  def evaluate(metrics: MetricStore): ResizeType

}

package com.amazonaws.emr.scaling.algo

import com.amazonaws.emr.Config
import com.amazonaws.emr.cluster.Workers
import com.amazonaws.emr.cluster.WorkersUnit.NODES
import org.apache.logging.log4j.scala.Logging

class StepScalingAlgorithm(
  override val evaluator: Evaluator,
  override val workers: Workers
) extends ScalingAlgorithm with Logging {

  private val minCapacity = workers.minCapacity
  private val maxCapacity = workers.maxCapacity
  private val capacityType = workers.units
  private val stepExpand = if (capacityType.equals(NODES)) Config.IgScaleStepExpand else Config.IfScaleStepExpand
  private val stepShrink = if (capacityType.equals(NODES)) Config.IgScaleStepShrink else Config.IfScaleStepShrink

  logger.info(s"Using ${this.getClass.getName} - min:$minCapacity max:$maxCapacity expand:$stepExpand shrink: $stepShrink unit: $capacityType")

  override def expand(running: Int, required: Int): Int = {
    val computedStep = (running + stepExpand).max(required)
    computedStep.min(maxCapacity)
  }

  override def shrink(running: Int): Int = (running - stepShrink).max(minCapacity)

}

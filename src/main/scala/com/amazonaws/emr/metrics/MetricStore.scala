package com.amazonaws.emr.metrics

import com.amazonaws.emr.Config.{MetricsDataPointsOneMin, MetricsMaxDataPoints}
import com.amazonaws.emr.utils.FixedList

class MetricStore {

  private val RawMetricStore = new FixedList[Map[String, Double]](MetricsMaxDataPoints)

  /** Add a new data point in memory */
  def append(metric: Map[String, Double]): Unit = RawMetricStore.append(metric)

  /** Return the last data point collected */
  def last: Map[String, Double] = RawMetricStore.last

  /** Return a boolean to determine if we have a sufficient number of data points to perform an evaluation */
  def isCold: Boolean = all.isEmpty

  /** Return data points collected. We explicitly filter nodes for which we haven't collected enough data points */
  private def all: Map[String, List[Double]] = getNodesList.map { node =>
    val points = RawMetricStore.map(d => d.getOrElse[Double](node, 0)).toList
    (node, points)
  }.filter(e => e._2.size >= MetricsDataPointsOneMin).toMap

  /** Compute average */
  private def average(ts: List[Double]): Double = ts.sum / ts.size

  /** Get a list of all the nodes using the last data point */
  private def getNodesList: List[String] = RawMetricStore
    .lastOption
    .getOrElse(List())
    .map(_._1)
    .toList

  /** Compute the average for each node */
  def oneMinuteAvg: Map[String, Double] = oneMinute.map(e => e._1 -> average(e._2))

  /** Return the last minute metrics */
  def oneMinute: Map[String, List[Double]] = all.map(e => e._1 -> e._2.takeRight(MetricsDataPointsOneMin))

  /**
   * Determine if the average of the specified fraction of nodes is between the specified interval
   *
   * @param fraction - nodes that should match the threshold condition (eg. 0.8 -> 80% of nodes)
   * @param min      - min value to trigger the condition
   * @param max      - max value to trigger the condition
   * @return boolean
   */
  def isAvgOneMinBetween(fraction: Double, min: Double, max: Double): Boolean = {
    val numBreached = oneMinuteAvg.map(p => (p._2 > min) && (p._2 < max)).count(_ == true)
    val numTotalFact = getNodesList.size * fraction
    numBreached >= numTotalFact
  }

  /**
   * Determine if the average of the specified fraction of nodes is greater than a threshold for one minute metrics
   *
   * @param fraction  - nodes that should match the threshold condition (eg. 0.8 -> 80% of nodes)
   * @param threshold - value to trigger the condition
   * @return boolean
   */
  def isAvgOneMinGreater(fraction: Double, threshold: Double): Boolean = {
    val numBreached = oneMinuteAvg.map(_._2 >= threshold).count(_ == true)
    val numTotalFact = getNodesList.size * fraction
    numBreached >= numTotalFact
  }

  /**
   * Determine if the average of the specified fraction of nodes is lower than a threshold for one minute metrics
   *
   * @param fraction  - nodes that should match the threshold condition (eg. 0.8 -> 80% of nodes)
   * @param threshold - value to trigger the condition
   * @return boolean
   */
  def isAvgOneMinLower(fraction: Double, threshold: Double): Boolean = !isAvgOneMinGreater(fraction, threshold)

}
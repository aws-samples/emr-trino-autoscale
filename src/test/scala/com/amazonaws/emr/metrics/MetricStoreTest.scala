package com.amazonaws.emr.metrics

import com.amazonaws.emr.Config
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

class MetricStoreTest extends AnyFunSuite with BeforeAndAfter with PrivateMethodTester {

  var metrics: MetricStore = _
  var singleData: Map[String, Double] = _
  var oneMinData: Map[String, List[Double]] = _

  before {
    metrics = new MetricStore
    singleData = Map("d1" -> 1.2)
    oneMinData = Map("d1" -> List(1.2, 1.2, 1.2, 1.2))
  }

  test("MetricStore.append") {
    metrics.append(singleData)
    assert(metrics.last === singleData)
  }

  test("MetricStore.last") {
    metrics.append(Map("d1" -> 1.2))
    metrics.append(Map("d2" -> 1.3))
    assert(metrics.last === Map("d2" -> 1.3))
  }

  test("MetricStore.isCold") {
    metrics.append(singleData)
    assert(metrics.isCold === true)
    metrics.append(singleData)
    metrics.append(singleData)
    metrics.append(singleData)
    assert(metrics.isCold === false)
  }

  test("MetricStore.average") {
    val average = PrivateMethod[Double]('average)
    assert(1.2 === (metrics invokePrivate average(List(1.2, 1.2))))
    assert(5.0 === (metrics invokePrivate average(List(2.0, 4.0, 6.0, 8.0))))
  }

  test("MetricStore.oneMinute") {
    metrics.append(singleData)
    metrics.append(singleData)
    assert(metrics.oneMinute == Map.empty)
    metrics.append(singleData)
    metrics.append(singleData)
    assert(metrics.oneMinute == oneMinData)
    assert(metrics.oneMinuteAvg === singleData)
  }

  test("MetricStore.isAvgOneMinGreater") {
    val data = Map("d1" -> 0.2, "d2" -> 0.8)
    (1 to Config.MetricsDataPointsOneMin).foreach(d => metrics.append(data))
    assert(!metrics.isCold)
    assert(metrics.isAvgOneMinGreater(1, 0.2) === true)
    assert(metrics.isAvgOneMinGreater(0.5, 0.8) === true)
  }

  test("MetricStore.isAvgOneMinBetween") {
    val data = Map("d1" -> 0.5, "d2" -> 0.6)
    (1 to Config.MetricsDataPointsOneMin).foreach(d => metrics.append(data))
    assert(!metrics.isCold)
    assert(metrics.isAvgOneMinBetween(1, 0.45, 0.65) === true)
    assert(metrics.isAvgOneMinBetween(0.5, 0.45, 0.65) === true)
    assert(metrics.isAvgOneMinBetween(0.5, 0.45, 0.55) === true)
    assert(metrics.isAvgOneMinBetween(0.5, 0.25, 0.35) === false)
  }

}

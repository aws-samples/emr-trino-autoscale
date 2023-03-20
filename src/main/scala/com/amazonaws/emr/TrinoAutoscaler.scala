package com.amazonaws.emr

import akka.actor.{ActorSystem, Scheduler}
import com.amazonaws.emr.Config.EmrClusterId
import com.amazonaws.emr.cluster.Workers
import com.amazonaws.emr.metrics.TrinoMetricStore
import com.amazonaws.emr.scaling.ScalingManager
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object TrinoAutoscaler extends App with Logging {

  implicit val system: ActorSystem = ActorSystem()
  implicit val executor: ExecutionContextExecutor = system.dispatcher
  implicit val scheduler: Scheduler = system.scheduler

  private val workers = Workers(EmrClusterId)
  private val scaling = new ScalingManager(workers)
  private val metrics = new TrinoMetricStore()

  system.scheduler.scheduleWithFixedDelay(
    initialDelay = 0 seconds,
    delay = Config.MetricsCollectInterval
  )(() => metrics.collect())

  system.scheduler.scheduleWithFixedDelay(
    initialDelay = Config.MetricsCollectInterval * Config.MetricsDataPointsOneMin,
    delay = Config.MetricsEvaluationInterval,
  )(() => scaling.evaluate(metrics))

}

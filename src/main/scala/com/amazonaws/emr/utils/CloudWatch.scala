package com.amazonaws.emr.utils

import com.amazonaws.ClientConfiguration
import com.amazonaws.emr.Config
import com.amazonaws.emr.Config.{CloudWatchDimension, CloudWatchNamespace, EmrClusterId}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import org.apache.logging.log4j.scala.Logging

object CloudWatch extends Logging {

  private val retryPolicy = RetryPolicy.builder().withMaxErrorRetry(Config.AwsSdkMaxRetry).build()
  private val clientConf = new ClientConfiguration().withRetryPolicy(retryPolicy)
  private val client = AmazonCloudWatchClientBuilder.standard().withClientConfiguration(clientConf).build

  def publish(prefix: String, data: Double): Unit = {

    logger.debug(s"Publish $prefix to CloudWatch. Namespace:$CloudWatchNamespace Dimension:$CloudWatchDimension=$EmrClusterId")

    val request = new PutMetricDataRequest()
      .withNamespace(CloudWatchNamespace)

    val dimension = new Dimension()
      .withName(CloudWatchDimension)
      .withValue(EmrClusterId)

    val datum = new MetricDatum()
      .withMetricName(s"$prefix")
      .withUnit(StandardUnit.Count)
      .withValue(data)
      .withDimensions(dimension)

    request.withMetricData(datum)
    client.putMetricData(request)
  }

  def publish(prefix: String, data: Map[String, Double]): Unit = {

    logger.debug(s"Publish $prefix to CloudWatch. Namespace:$CloudWatchNamespace Dimension:$CloudWatchDimension=$EmrClusterId")

    val request = new PutMetricDataRequest()
      .withNamespace(CloudWatchNamespace)

    val dimension = new Dimension()
      .withName(CloudWatchDimension)
      .withValue(EmrClusterId)

    data.foreach { e =>
      val datum = new MetricDatum()
        .withMetricName(s"$prefix.${e._1}")
        .withUnit(StandardUnit.Count)
        .withValue(e._2)
        .withDimensions(dimension)
      request.withMetricData(datum)
    }
    client.putMetricData(request)
  }

}

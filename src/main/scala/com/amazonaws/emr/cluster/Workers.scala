package com.amazonaws.emr.cluster

import akka.actor.{ActorSystem, Scheduler}
import com.amazonaws.ClientConfiguration
import com.amazonaws.emr.Config
import com.amazonaws.emr.TrinoAutoscaler.{scheduler, system}
import com.amazonaws.emr.cluster.WorkersUnit.WorkerUnit
import com.amazonaws.emr.utils.FixedList
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model._
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Main entry point for controlling workers nodes in an Amazon EMR cluster. The class provides core methods to monitor
 * and modify the status of TASK nodes in the cluster. This might be extended to CORE nodes as well, but considering
 * that is more efficient and fast to scale on TASK nodes, CORE nodes are not considered for autoscaling purposes.
 */
abstract class Workers(implicit val system: ActorSystem, implicit val scheduler: Scheduler) {

  val minCapacity: Int
  val maxCapacity: Int
  val units: WorkerUnit
  val label: String

  protected val clusterId: String
  protected val defaultMarket: String
  protected val managed: FixedList[TaskRunning] = new FixedList[TaskRunning](Config.MaxInstanceGroupsLimit)
  protected val managedTaskStatus: FixedList[TaskState] = new FixedList[TaskState](Config.MaxInstanceGroupsLimit)
  protected val suspendedStateDelayMs: Long = 10.minute.toMillis

  // Amazon EMR SDK client
  private val retryPolicy = RetryPolicy.builder().withMaxErrorRetry(Config.AwsSdkMaxRetry).build()
  private val clientConf = new ClientConfiguration().withRetryPolicy(retryPolicy)
  private val client = AmazonElasticMapReduceClientBuilder.standard().withClientConfiguration(clientConf).build

  // initialize a scheduled task to refresh the status of TASK groups / fleets
  scheduler.scheduleWithFixedDelay(initialDelay = 30 seconds, delay = 30 seconds)(refresh())

  protected def initialize(): scala.Unit

  protected def create(taskSpec: TaskSpec, taskRunning: List[TaskRunning]): scala.Unit

  protected def list(): List[TaskRunning]

  def resize(count: Int): scala.Unit

  protected def refresh(): Runnable

  def running: Int = managedTaskStatus.map(_.running).sum

  def requested: Int = managedTaskStatus.map(_.requested).sum

  def isResizing: Boolean = managedTaskStatus.exists(_.isResizing)

}

object Workers extends Logging {

  private val retryPolicy = RetryPolicy.builder().withMaxErrorRetry(Config.AwsSdkMaxRetry).build()
  private val clientConf = new ClientConfiguration().withRetryPolicy(retryPolicy)
  private val client = AmazonElasticMapReduceClientBuilder.standard().withClientConfiguration(clientConf).build

  /** Factory implementation for Workers */
  def apply(clusterId: String): Workers = {
    if (isInstanceFleet(clusterId)) new IfWorkers(clusterId) else new IgWorkers(clusterId)
  }

  /** Check if the cluster uses instance fleets */
  private def isInstanceFleet(clusterId: String): Boolean = {
    val request = new DescribeClusterRequest().withClusterId(clusterId)
    val response = client.describeCluster(request)
    response.getCluster.getInstanceCollectionType.equalsIgnoreCase(InstanceCollectionType.INSTANCE_FLEET.toString)
  }

  /**
   * Amazon EMR Instance Groups implementation.
   *
   * @param clusterId Amazon EMR JobFlowId e.g. j-xxxxxxxxx
   */
  private class IgWorkers(override val clusterId: String) extends Workers with Logging {
    override val defaultMarket: String = if (Config.IgShouldUseSpot) MarketType.SPOT.toString else MarketType.ON_DEMAND.toString
    override val minCapacity: Int = Config.IgMinNumNodes
    override val maxCapacity: Int = Config.IgMaxNumNodes
    override val units: WorkerUnit = WorkersUnit.NODES
    override val label: String = "Instance Group"

    initialize()

    /** Initialize managed workers */
    override protected def initialize(): scala.Unit = {
      logger.info(s"Initializing $label")
      val tasks = Config.IgInstanceTypes.map(i =>
        TaskSpec(s"${Config.WorkersNamePrefix}-$defaultMarket-$i", List(Instance(i, 1, defaultMarket)))
      )
      tasks.foreach(t => create(t, list()))
      logger.info(s"Managed $label: ${managed.map(_.id).mkString("", ", ", "")}")
    }

    /** Create a new TASK Instance Group */
    override protected def create(taskSpec: TaskSpec, taskRunning: List[TaskRunning]): scala.Unit = {

      val validTask = taskRunning
        .filter(g => g.name.equalsIgnoreCase(taskSpec.name))
        .filter(g => g.instances.head.name.equalsIgnoreCase(taskSpec.instances.head.name))
        .find(g => g.instances.head.market.equalsIgnoreCase(taskSpec.instances.head.market))

      if (validTask.nonEmpty) {
        logger.info(s"Existing $label matching requirements: ${validTask.get}")
        managed.append(validTask.get)
      } else {

        logger.info(s"Creating $label with: $taskSpec")

        // Check if the total number of groups is going to breach EMR service limits
        val totalGroups = taskRunning.size + 1
        if (totalGroups > Config.MaxInstanceGroupsLimit) {
          throw new RuntimeException(
            s"""
               |You can have a maximum number of ${Config.MaxInstanceGroupsLimit} TASK $label
               |existing: ${taskRunning.length}
               |""".stripMargin
          )
        }

        val igConf = new InstanceGroupConfig()
          .withName(taskSpec.name)
          .withMarket(taskSpec.instances.head.market)
          .withInstanceType(taskSpec.instances.head.name)
          .withInstanceRole(InstanceRoleType.TASK.toString)
          .withInstanceCount(0)

        val request = new AddInstanceGroupsRequest()
          .withJobFlowId(clusterId)
          .withInstanceGroups(igConf)

        val response = client.addInstanceGroups(request)
        val id = response.getInstanceGroupIds.asScala.head
        val instance = taskSpec.instances.head

        managed.append(TaskRunning(id, taskSpec.name, List(Instance(instance.name, 1, instance.market))))
      }
    }

    /** List Running TASK Instance Groups */
    override protected def list(): List[TaskRunning] = {
      val request = new ListInstanceGroupsRequest().withClusterId(clusterId)
      client.listInstanceGroups(request)
        .getInstanceGroups.asScala.toList
        .filter(_.getInstanceGroupType.equalsIgnoreCase(InstanceRoleType.TASK.toString))
        .map(g => TaskRunning(g.getId, g.getName, List(Instance(g.getInstanceType, 1, g.getMarket))))
    }

    /**
     * Resize TASK Instance Groups
     *
     * If concurrent scaling is enabled, multiple groups scale at the same time. This helps to speed up scaling
     * in case of ICE issues. If concurrent scaling is disabled, the first managed IG is used for scaling.
     *
     * @param count number of instances requested
     */
    override def resize(count: Int): scala.Unit = {
      logger.debug(s"Received scaling request: $count ($units)")

      running - count match {
        case x if x > 0 =>
          if (Config.IgScaleConcurrently) {
            logger.debug(s"Shrinking - All Groups Considered: ${managedTaskStatus.toList}")
            resizeConcurrently(managedTaskStatus.toList, count)
          } else {
            logger.debug(s"Shrinking - Considering only: ${managedTaskStatus.head.id}")
            resizeGroup(managedTaskStatus.head.id, count)
          }

        case x if x < 0 =>
          if (Config.IgScaleConcurrently) {
            logger.debug(s"Expanding - Excluding Blocked Groups: ${managedTaskStatus.filter(_.isExcluded).toList}")
            resizeConcurrently(managedTaskStatus.filterNot(_.isExcluded).toList, count)
          } else {
            logger.debug(s"Expanding - Considering only: ${managedTaskStatus.head.id}")
            resizeGroup(managedTaskStatus.head.id, count)
          }
        case _ =>
          logger.debug("Nothing to do")
      }

    }

    /**
     * Resize a single Instance Group using Amazon EMR SDK API.
     *
     * @param id    Id of the IG to modify
     * @param count Final count of running instances
     */
    private def resizeGroup(id: String, count: Int): scala.Unit = {
      logger.debug(s"Scaling $id to $count ($units)")
      val config = new InstanceGroupModifyConfig()
        .withInstanceGroupId(id)
        .withInstanceCount(count)
      val request = new ModifyInstanceGroupsRequest().withClusterId(clusterId).withInstanceGroups(config)
      client.modifyInstanceGroups(request)
    }

    /**
     * Logic to implement a concurrent resize across multiple IGs.
     *
     * @param groups List of IG to consider during the resize operation
     * @param count  Number of instances requested
     */
    private def resizeConcurrently(groups: List[TaskState], count: Int): scala.Unit = {
      val distribution = distribute(count, groups.size)
      val distributionList = groups.map(_.id).zip(distribution)
      logger.debug(s"Resize $count across ${groups.size} groups: $distribution")
      logger.debug(s"Distribution: ${distributionList.mkString("", ", ", "")}")
      distributionList.foreach(g => resizeGroup(g._1, g._2))
    }

    /**
     * Create a distribution list to uniformly spread a number across a fixed number of groups.
     * For example: Distribute 5 in 3 groups (2,2,1)
     *
     * @param value  Total number of instances to distribute
     * @param groups Number of groups
     * @return Distribution List
     */
    private def distribute(value: Int, groups: Int): List[Int] = {
      val integerPart: Int = value / groups
      val modulo = value % groups
      val distribution = for (i <- 0 until groups) yield {
        if (i < modulo) integerPart + 1 else integerPart
      }
      distribution.toList
    }

    /** Refresh the status of the managed Instance Groups */
    override protected def refresh(): Runnable = () => {

      val request = new ListInstanceGroupsRequest().withClusterId(clusterId)
      val response = client.listInstanceGroups(request)
      val status = response.getInstanceGroups.asScala.toList
        .filter(g => managed.map(_.id).toList.contains(g.getId))
        .map { g =>
          val id = g.getId
          val status = g.getStatus.getState
          val isResizing = g.getStatus.getState.equalsIgnoreCase(InstanceGroupState.RESIZING.toString)
          val isSuspended = g.getStatus.getState.equalsIgnoreCase(InstanceGroupState.SUSPENDED.toString)
          val running = g.getRunningInstanceCount
          val requested = g.getRequestedInstanceCount
          val message = g.getStatus.getStateChangeReason.getMessage

          val prevSuspendedTime = managedTaskStatus
            .filter(_.id == g.getId)
            .map(_.lastSuspendedTimeMs)
            .headOption.getOrElse(0L)

          val lastSuspendedTimeMs = if (isSuspended) System.currentTimeMillis() else prevSuspendedTime
          val isExcluded = if (System.currentTimeMillis() - prevSuspendedTime > suspendedStateDelayMs) false else true

          logger.info(s"$label status")
          logger.info(s"Task Group Status - id: $id status: $status lastSuspendedTimeMs: $lastSuspendedTimeMs")
          logger.info(s"Task Group Status - isResizing: $isResizing isSuspended: $isSuspended isExcluded: $isExcluded")
          logger.info(s"Task Group Status - running: $running requested: $requested message: $message")
          logger.info(s"Task Group Status - message: $message")

          TaskState(id, status, isResizing, isSuspended, isExcluded, running, requested, message, lastSuspendedTimeMs)
        }

      // update status
      managedTaskStatus.replaceAll(status)

      // Fix suspended TASK Groups
      managedTaskStatus.filter(_.isSuspended).foreach(g => resizeGroup(g.id, g.running))

    }

  }

  /**
   * Amazon EMR Instance Fleet implementation.
   *
   * @param clusterId Amazon EMR JobFlowId e.g. j-xxxxxxxxx
   */
  private class IfWorkers(override val clusterId: String) extends Workers with Logging {
    override protected val defaultMarket: String = if (Config.IfShouldUseSpot) MarketType.SPOT.toString else MarketType.ON_DEMAND.toString
    override val minCapacity: Int = Config.IfMinNumUnits
    override val maxCapacity: Int = Config.IfMaxNumUnits
    override val units: WorkerUnit = WorkersUnit.UNITS
    override val label: String = "Instance Fleet"

    initialize()

    /** Initialize Instance Fleet */
    override protected def initialize(): scala.Unit = {
      logger.info(s"Initializing $label")
      val instanceTypes = Config.IfInstanceTypes
      val instanceWights = Config.IfInstanceTypesUnits.map(_.toInt)
      val instances = instanceTypes zip instanceWights

      // Sanity checks
      if (instanceTypes.isEmpty || instanceWights.isEmpty) {
        throw new RuntimeException("Wrong Instance Fleet configurations")
      }
      if (instanceTypes.size != instanceWights.size) {
        throw new RuntimeException("Wrong Instance Fleet configurations")
      }

      val taskSpec = TaskSpec(Config.WorkersNamePrefix, instances.map(i => Instance(i._1, i._2, defaultMarket)))
      create(taskSpec, list())
      logger.info(s"Managed $label: ${managed.map(_.id).mkString("", ", ", "")}")
    }

    /** Create a new TASK Instance Fleet */
    override protected def create(taskSpec: TaskSpec, taskRunning: List[TaskRunning]): scala.Unit = {

      val validTask = taskRunning
        .filter(g => g.name.equalsIgnoreCase(taskSpec.name))
        .find(g => taskSpec.instances.map(_.name).forall(i => g.instances.map(_.name).contains(i)))

      if (validTask.nonEmpty) {
        logger.info(s"Existing $label matching requirements: ${validTask.get}")
        managed.append(validTask.get)
      } else if (validTask.isEmpty && taskRunning.nonEmpty) {
        throw new RuntimeException(s"Existing TASK $label doesn't match scaling specifications")
      } else {
        logger.info(s"Creating $label with: $taskSpec")
        val (onDemandCapacity, spotCapacity) = if (Config.IfShouldUseSpot) (0, Config.IfMinNumUnits) else (Config.IfMinNumUnits, 0)
        val instanceTypeConf = taskSpec.instances.map(i => new InstanceTypeConfig().withInstanceType(i.name).withWeightedCapacity(i.weight))
        val fleetConfig = new InstanceFleetConfig()
          .withName(taskSpec.name)
          .withInstanceFleetType(InstanceFleetType.TASK)
          .withTargetSpotCapacity(spotCapacity)
          .withTargetOnDemandCapacity(onDemandCapacity)
          .withInstanceTypeConfigs(instanceTypeConf.asJava)

        val request = new AddInstanceFleetRequest()
          .withClusterId(clusterId)
          .withInstanceFleet(fleetConfig)

        val response = client.addInstanceFleet(request)

        managed.append(TaskRunning(response.getInstanceFleetId, taskSpec.name, taskSpec.instances))
      }
    }

    /** List active TASK Instance Fleet */
    override protected def list(): List[TaskRunning] = {
      val request = new ListInstanceFleetsRequest().withClusterId(clusterId)
      client.listInstanceFleets(request)
        .getInstanceFleets.asScala.toList
        .filter(_.getInstanceFleetType.equalsIgnoreCase(InstanceRoleType.TASK.toString))
        .map { f =>
          val instances = f
            .getInstanceTypeSpecifications.asScala
            .map(i => Instance(i.getInstanceType, i.getWeightedCapacity, defaultMarket))
            .toList
          TaskRunning(f.getId, f.getName, instances)
        }
    }

    /** Resize TASK Instance Fleet */
    override def resize(count: Int): scala.Unit = {
      val taskId = managed.head.id
      val (onDemandCapacity, spotCapacity) = if (Config.IfShouldUseSpot) (0, count) else (count, 0)

      val config = new InstanceFleetModifyConfig()
        .withInstanceFleetId(taskId)
        .withTargetOnDemandCapacity(onDemandCapacity)
        .withTargetSpotCapacity(spotCapacity)

      val request = new ModifyInstanceFleetRequest().withClusterId(clusterId).withInstanceFleet(config)
      client.modifyInstanceFleet(request)
    }

    /** Refresh the status of the managed Instance Fleet */
    override protected def refresh(): Runnable = () => {
      val request = new ListInstanceFleetsRequest().withClusterId(clusterId)
      val response = client.listInstanceFleets(request)

      val status = response.getInstanceFleets.asScala.toList
        .filter(g => managed.map(_.id).toList.contains(g.getId))
        .map { g =>
          val id = g.getId
          val status = g.getStatus.getState
          val isResizing = g.getStatus.getState.equalsIgnoreCase(InstanceFleetState.RESIZING.toString)
          val isSuspended = g.getStatus.getState.equalsIgnoreCase(InstanceFleetState.SUSPENDED.toString)
          val message = g.getStatus.getStateChangeReason.getMessage

          val (running, requested) =
            if (Config.IfShouldUseSpot)
              (g.getProvisionedSpotCapacity, g.getTargetSpotCapacity)
            else
              (g.getProvisionedOnDemandCapacity, g.getTargetOnDemandCapacity)

          val isExcluded = false
          val lastSuspendedTimeMs = 0L

          logger.info(s"$label status")
          logger.info(s"Task Fleet Status - id: $id status: $status lastSuspendedTimeMs: $lastSuspendedTimeMs")
          logger.info(s"Task Fleet Status - isResizing: $isResizing isSuspended: $isSuspended isExcluded: $isExcluded")
          logger.info(s"Task Fleet Status - running: $running requested: $requested message: $message")
          logger.info(s"Task Fleet Status - message: $message")

          TaskState(id, status, isResizing, isSuspended, isExcluded, running, requested, message, lastSuspendedTimeMs)
        }

      managedTaskStatus.replaceAll(status)

    }
  }
}


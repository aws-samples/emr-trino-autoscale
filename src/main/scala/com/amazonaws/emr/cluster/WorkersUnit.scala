package com.amazonaws.emr.cluster

/**
 * Enumeration of Workers unit types
 *
 * - NODES are used when working with EMR Instance Groups
 * - UNITS are used when working with EMR Instance Fleets
 */
object WorkersUnit extends Enumeration {
  type WorkerUnit = Value

  val NODES,
      UNITS = Value
}
package com.amazonaws.emr.cluster

/**
 * Details about an instance type used by a group or fleet
 *
 * @param name   Name of the instance type e.g. m5.xlarge
 * @param weight For Instance Fleets clusters this represents the weight of the instance (EMR standard behaviour)
 *               For Instance Groups clusters this represents a dummy weight
 * @param market Default market used for the instance: ON_DEMAND, SPOT, MIXED
 */
case class Instance(name: String, weight: Int, market: String) {
  override def toString: String = s"$name (w: $weight m: $market)"
}

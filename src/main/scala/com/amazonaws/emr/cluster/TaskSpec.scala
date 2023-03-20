package com.amazonaws.emr.cluster

case class TaskSpec(name: String, instances: List[Instance]) {
  override def toString: String = s"name: $name instances: ${instances.mkString("", ",", "")}"
}
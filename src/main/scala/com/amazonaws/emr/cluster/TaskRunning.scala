package com.amazonaws.emr.cluster

case class TaskRunning(id: String, name: String, instances: List[Instance]) {
  override def toString: String = s"$id - $name (${instances.map(_.name).mkString("", ", ", "")})"
}

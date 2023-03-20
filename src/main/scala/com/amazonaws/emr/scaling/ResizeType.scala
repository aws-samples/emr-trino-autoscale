package com.amazonaws.emr.scaling

object ResizeType extends Enumeration {
  type ResizeType = Value

  val EXPAND,
      SHRINK,
      NONE = Value
}
package com.amazonaws.emr.scaling

import com.amazonaws.emr.scaling.ResizeType.ResizeType

case class ResizeAction(action: ResizeType, capacity: Int)
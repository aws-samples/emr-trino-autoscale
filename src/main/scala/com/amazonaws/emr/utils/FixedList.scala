package com.amazonaws.emr.utils

import scala.collection.Traversable
import scala.collection.mutable.ListBuffer

class FixedList[A](size: Int) extends Traversable[A] {

  private val list: ListBuffer[A] = ListBuffer()

  def appendAll(l: Traversable[A]): Unit = l.foreach(append)

  def append(elem: A): Unit = {
    if (list.size == size) list.trimStart(1)
    list.append(elem)
  }

  def replaceAll(l: Traversable[A]): Unit = {
    list.clear()
    list.appendAll(l)
  }

  def foreach[U](f: A => U): Unit = list.foreach(f)

}
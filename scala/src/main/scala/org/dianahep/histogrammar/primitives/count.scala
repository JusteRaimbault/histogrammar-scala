package org.dianahep

import org.dianahep.histogrammar.json._

package histogrammar {
  //////////////////////////////////////////////////////////////// Count/Counted/Counting

  object Count extends Factory {
    val name = "Count"
    val help = "Count data, ignoring their content."
    val detailedHelp = """Count()"""

    def container(value: Long) = new Counted(value)
    def apply() = new Counting(0L)

    def unapply(x: Counted) = Some(x.value)
    def unapply(x: Counting) = Some(x.value)

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonInt(value) => new Counted(value)
      case _ => throw new JsonFormatException(json, name)
    }
  }

  class Counted(val value: Long) extends Container[Counted] {
    def factory = Count

    def +(that: Counted): Counted = new Counted(this.value + that.value)

    def toJsonFragment = JsonInt(value)

    override def toString() = s"Counted"
    override def equals(that: Any) = that match {
      case that: Counted => this.value == that.value
      case _ => false
    }
    override def hashCode() = value.hashCode
  }

  class Counting(var value: Long) extends Aggregator[Any, Counting] {
    def factory = Count

    def +(that: Counting): Counting = new Counting(this.value + that.value)

    def fillWeighted[SUB <: Any](datum: SUB, weight: Double) {
      value += 1L
    }

    def toJsonFragment = JsonInt(value)

    override def toString() = s"Counting"
    override def equals(that: Any) = that match {
      case that: Counting => this.value == that.value
      case _ => false
    }
    override def hashCode() = value.hashCode
  }
}

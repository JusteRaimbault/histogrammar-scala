package org.dianahep

import org.dianahep.histogrammar.json._

package histogrammar {
  //////////////////////////////////////////////////////////////// Partition/Partitioned/Partitioning

  object Partition extends Factory {
    val name = "Partition"
    val help = "Accumulate a suite containers, filling the one that is between a pair of given cuts on a given expression."
    val detailedHelp = """Partition(value: => V, expression: NumericalFcn[DATUM], cuts: Double*)"""

    def fixed[V <: Container[V]](cuts: (Double, V)*) = new Partitioned(cuts: _*)
    def apply[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}](value: => V, expression: NumericalFcn[DATUM], cuts: Double*) =
      new Partitioning(expression, (java.lang.Double.NEGATIVE_INFINITY +: cuts).map((_, value)): _*)

    def unapplySeq[V <: Container[V]](x: Partitioned[V]) = Some(x.cuts)
    def unapplySeq[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}](x: Partitioning[DATUM, V]) = Some(x.cuts)

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("type", "data")) =>
        val get = pairs.toMap

        val factory = get("type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".type")
        }

        get("data") match {
          case JsonArray(elements @ _*) if (elements.size >= 1) =>
            new Partitioned[Container[_]](elements.zipWithIndex map {case (element, i) =>
              element match {
                case JsonObject(elementPairs @ _*) if (elementPairs.keySet == Set("atleast", "data")) =>
                  val elementGet = elementPairs.toMap
                  val atleast = elementGet("atleast") match {
                    case JsonNumber(x) => x
                    case x => throw new JsonFormatException(x, name + s".data element $i atleast")
                  }
                  (atleast, factory.fromJsonFragment(elementGet("data")))

                case x => throw new JsonFormatException(x, name + s".data element $i")
              }
            }: _*)

          case x => throw new JsonFormatException(x, name + ".data")
        }

      case _ => throw new JsonFormatException(json, name)
    }
  }

  class Partitioned[V <: Container[V]](val cuts: (Double, V)*) extends Container[Partitioned[V]] {
    type Type = Partitioned[V]
    def factory = Partition

    if (cuts.size < 1)
      throw new ContainerException(s"number of cuts (${cuts.size}) must be at least 1 (including the implicit >= -inf, which the Partition.ing factory method adds)")

    def +(that: Partitioned[V]) =
      if (this.cuts.size != that.cuts.size)
        throw new ContainerException(s"cannot add Partitioned because the number of cut differs (${this.cuts.size} vs ${that.cuts.size})")
      else
        new Partitioned(this.cuts zip that.cuts map {case ((mycut, me), (yourcut, you)) =>
          if (mycut != yourcut)
            throw new ContainerException(s"cannot add Partitioned because cut differs ($mycut vs $yourcut)")
          (mycut, me + you)
        }: _*)

    def toJsonFragment = JsonObject(
      "type" -> JsonString(cuts.head._2.factory.name),
      "data" -> JsonArray(cuts map {case (atleast, sub) => JsonObject("atleast" -> JsonFloat(atleast), "data" -> sub.toJsonFragment)}: _*))

    override def toString() = s"""Partitioned[${cuts.head._2}, cuts=[${cuts.map(_._1).mkString(", ")}]]"""
    override def equals(that: Any) = that match {
      case that: Partitioned[V] => this.cuts zip that.cuts forall {case (me, you) => me._1 === you._1  &&  me._2 == you._2}
      case _ => false
    }
  }

  class Partitioning[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}](val expression: NumericalFcn[DATUM], val cuts: (Double, V)*) extends Container[Partitioning[DATUM, V]] with Aggregation {
    type Type = Partitioning[DATUM, V]
    type Datum = DATUM
    def factory = Partition

    if (cuts.size < 1)
      throw new ContainerException(s"number of cuts (${cuts.size}) must be at least 1 (including the implicit >= -inf, which the Partition.ing factory method adds)")

    private val range = cuts zip (cuts.tail :+ (java.lang.Double.NaN, null))

    def +(that: Partitioning[DATUM, V]) =
      if (this.cuts.size != that.cuts.size)
        throw new ContainerException(s"cannot add Partitioning because the number of cut differs (${this.cuts.size} vs ${that.cuts.size})")
      else
        new Partitioning(this.expression,
          this.cuts zip that.cuts map {case ((mycut, me), (yourcut, you)) =>
            if (mycut != yourcut)
              throw new ContainerException(s"cannot add Partitioning because cut differs ($mycut vs $yourcut)")
            (mycut, me + you)
          }: _*)

    def fillWeighted[SUB <: Datum](datum: SUB, weight: Double) {
      if (weight > 0.0) {
        val value = expression(datum)
        // !(value >= high) is true when high == NaN (even if value == +inf)
        range find {case ((low, sub), (high, _)) => value >= low  &&  !(value >= high)} foreach {case ((_, sub), (_, _)) =>
          sub.fillWeighted(datum, weight)
        }
      }
    }

    def toJsonFragment = JsonObject(
      "type" -> JsonString(cuts.head._2.factory.name),
      "data" -> JsonArray(cuts map {case (atleast, sub) => JsonObject("atleast" -> JsonFloat(atleast), "data" -> sub.toJsonFragment)}: _*))

    override def toString() = s"""Partitioning[${cuts.head._2}, cuts=[${cuts.map(_._1).mkString(", ")}]]"""
    override def equals(that: Any) = that match {
      case that: Partitioning[DATUM, V] => this.expression == that.expression  &&  (this.cuts zip that.cuts forall {case (me, you) => me._1 === you._1  &&  me._2 == you._2})
      case _ => false
    }
  }
}

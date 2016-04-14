package org.dianahep

import org.dianahep.histogrammar.json._

package histogrammar {
  //////////////////////////////////////////////////////////////// Sum/Summed/Summing

  object Sum extends Factory {
    val name = "Sum"
    val help = "Accumulate the sum of a given quantity."
    val detailedHelp = """Sum(quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM])"""

    def fixed(entries: Double, sum: Double) = new Summed(entries, sum)
    def apply[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = new Summing[DATUM](quantity, selection, 0.0, 0.0)

    def unapply(x: Summed) = Some((x.entries, x.sum))
    def unapply(x: Summing[_]) = Some((x.entries, x.sum))

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("entries", "sum")) =>
        val get = pairs.toMap

        val entries = get("entries") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".entries")
        }

        val sum = get("sum") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".sum")
        }

        new Summed(entries, sum)

      case _ => throw new JsonFormatException(json, name)
    }
  }

  class Summed(val entries: Double, val sum: Double) extends Container[Summed] {
    type Type = Summed
    def factory = Sum

    def zero = new Summed(0.0, 0.0)
    def +(that: Summed) = new Summed(this.entries + that.entries, this.sum + that.sum)

    def toJsonFragment = JsonObject("entries" -> JsonFloat(entries), "sum" -> JsonFloat(sum))

    override def toString() = s"Summed($sum)"
    override def equals(that: Any) = that match {
      case that: Summed => this.entries === that.entries  &&  this.sum === that.sum
      case _ => false
    }
    override def hashCode() = (entries, sum).hashCode
  }

  class Summing[DATUM](val quantity: NumericalFcn[DATUM], val selection: Selection[DATUM], var entries: Double, var sum: Double) extends Container[Summing[DATUM]] with AggregationOnData {
    type Type = Summing[DATUM]
    type Datum = DATUM
    def factory = Sum

    def zero = new Summing[DATUM](quantity, selection, 0.0, 0.0)
    def +(that: Summing[DATUM]) = new Summing(this.quantity, this.selection, this.entries + that.entries, this.sum + that.sum)

    def fillWeighted[SUB <: Datum](datum: SUB, weight: Double) {
      val w = weight * selection(datum)
      if (w > 0.0) {
        val q = quantity(datum)
        entries += w
        sum += q * w
      }
    }

    def toJsonFragment = JsonObject("entries" -> JsonFloat(entries), "sum" -> JsonFloat(sum))

    override def toString() = s"Summing($sum)"
    override def equals(that: Any) = that match {
      case that: Summing[DATUM] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.entries === that.entries  &&  this.sum === that.sum
      case _ => false
    }
    override def hashCode() = (quantity, selection, entries, sum).hashCode
  }
}

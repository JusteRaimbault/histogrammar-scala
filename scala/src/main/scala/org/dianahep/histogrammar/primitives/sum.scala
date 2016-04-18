// Copyright 2016 Jim Pivarski
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.dianahep

import org.dianahep.histogrammar.json._

package histogrammar {
  //////////////////////////////////////////////////////////////// Sum/Summed/Summing

  /** Accumulate the sum of a given quantity.
    * 
    * Factory produces mutable [[org.dianahep.histogrammar.Summing]] and immutable [[org.dianahep.histogrammar.Summed]] objects.
    */
  object Sum extends Factory {
    val name = "Sum"
    val help = "Accumulate the sum of a given quantity."
    val detailedHelp = """Sum(quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM])"""

    /** Create an immutable [[org.dianahep.histogrammar.Summed]] from arguments (instead of JSON).
      * 
      * @param entries weighted number of entries (sum of all observed weights)
      * @param sum the sum of weight times quantity over all entries
      */
    def ed(entries: Double, sum: Double) = new Summed(entries, sum)

    /** Create an empty, mutable [[org.dianahep.histogrammar.Summing]].
      * 
      * @param quantity numerical function to track
      * @param selection boolean or non-negative function that cuts or weights entries
      */
    def apply[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = new Summing[DATUM](quantity, selection, 0.0, 0.0)

    /** Synonym for `apply`. */
    def ing[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = apply(quantity, selection)

    /** Use [[org.dianahep.histogrammar.Summed]] in Scala pattern-matching. */
    def unapply(x: Summed) = Some((x.entries, x.sum))
    /** Use [[org.dianahep.histogrammar.Summing]] in Scala pattern-matching. */
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

  /** An accumulated weighted sum of a given quantity.
    * 
    * @param entries weighted number of entries (sum of all observed weights)
    * @param sum the sum of weight times quantity over all entries
    */
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

  /** Accumulating a weighted sum of a given quantity.
    * 
    * @param quantity numerical function to track
    * @param selection boolean or non-negative function that cuts or weights entries
    * @param entries weighted number of entries (sum of all observed weights)
    * @param sum the sum of weight times quantity over all entries
    */
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

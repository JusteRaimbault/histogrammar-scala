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
  //////////////////////////////////////////////////////////////// Bin/Binned/Binning

  /** Split a given quantity into equally spaced bins between specified limits and fill only one bin per datum.
    * 
    * Factory produces mutable [[org.dianahep.histogrammar.Binning]] and immutable [[org.dianahep.histogrammar.Binned]] objects.
    */
  object Bin extends Factory {
    val name = "Bin"
    val help = "Split a given quantity into equally spaced bins between specified limits and fill only one bin per datum."
    val detailedHelp ="""Bin(num: Int, low: Double, high: Double, quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM],
           value: => V = Count(), underflow: U = Count(), overflow: O = Count(), nanflow: N = Count())"""

    /** Create an immutable [[org.dianahep.histogrammar.Binned]] from arguments (instead of JSON).
      * 
      * @param low Minimum-value edge of the first bin.
      * @param high Maximum-value edge of the last bin.
      * @param entries Weighted number of entries (sum of all observed weights).
      * @param values Containers for data sent to each bin.
      * @param underflow Container for data below the first bin.
      * @param overflow Container for data above the last bin.
      * @param nanflow Container for data that resulted in `NaN`.
      */
    def ed[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]]
      (low: Double,
       high: Double,
       entries: Double,
       values: Seq[V],
       underflow: U,
       overflow: O,
       nanflow: N) = new Binned[V, U, O, N](low, high, entries, values, underflow, overflow, nanflow)

    /** Create an empty, mutable [[org.dianahep.histogrammar.Binning]].
      * 
      * @param number Of bins.
      * @param low Minimum-value edge of the first bin.
      * @param high Maximum-value edge of the last bin.
      * @param value New value (note the `=>`: expression is reevaluated every time a new value is needed).
      * @param underflow Container for data below the first bin.
      * @param overflow Container for data above the last bin.
      * @param nanflow Container for data that resulted in `NaN`.
      */
    def apply[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, U <: Container[U] with Aggregation{type Datum >: DATUM}, O <: Container[O] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}]
      (num: Int,
       low: Double,
       high: Double,
       quantity: NumericalFcn[DATUM],
       selection: Selection[DATUM] = unweighted[DATUM],
       value: => V = Count(),
       underflow: U = Count(),
       overflow: O = Count(),
       nanflow: N = Count()) =
      new Binning[DATUM, V, U, O, N](low, high, quantity, selection, 0.0, Seq.fill(num)(value), underflow, overflow, nanflow)

    /** Synonym for `apply`. */
    def ing[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, U <: Container[U] with Aggregation{type Datum >: DATUM}, O <: Container[O] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}]
      (num: Int,
       low: Double,
       high: Double,
       quantity: NumericalFcn[DATUM],
       selection: Selection[DATUM] = unweighted[DATUM],
       value: => V = Count(),
       underflow: U = Count(),
       overflow: O = Count(),
       nanflow: N = Count()) = apply(num, low, high, quantity, selection, value, underflow, overflow, nanflow)

    /** Use [[org.dianahep.histogrammar.Binned]] in Scala pattern-matching. */
    def unapply[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]](x: Binned[V, U, O, N]) = Some((x.entries, x.values, x.underflow, x.overflow, x.nanflow))
    /** Use [[org.dianahep.histogrammar.Binning]] in Scala pattern-matching. */
    def unapply[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, U <: Container[U] with Aggregation{type Datum >: DATUM}, O <: Container[O] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}](x: Binning[DATUM, V, U, O, N]) = Some((x.entries, x.values, x.underflow, x.overflow, x.nanflow))

    trait Methods {
      /** Number of bins. */
      def num: Int
      def low: Double
      def high: Double

      /** Find the bin index associated with numerical value `x`.
        * 
        * @return -1 if `x` is out of range; the bin index otherwise.
        */
      def bin(x: Double): Int =
        if (under(x)  ||  over(x)  ||  nan(x))
          -1
        else
          Math.floor(num * (x - low) / (high - low)).toInt

      /** Return `true` iff `x` is in the underflow region (less than `low`). */
      def under(x: Double): Boolean = !x.isNaN  &&  x < low
      /** Return `true` iff `x` is in the overflow region (greater than `high`). */
      def over(x: Double): Boolean = !x.isNaN  &&  x >= high
      /** Return `true` iff `x` is in the nanflow region (equal to `NaN`). */
      def nan(x: Double): Boolean = x.isNaN

      /** Get a sequence of valid indexes. */
      def indexes: Seq[Int] = 0 until num
      /** Get the low and high edge of a bin (given by index number). */
      def range(index: Int): (Double, Double) = ((high - low) * index / num + low, (high - low) * (index + 1) / num + low)
    }

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("low", "high", "entries", "values:type", "values", "underflow:type", "underflow", "overflow:type", "overflow", "nanflow:type", "nanflow")) =>
        val get = pairs.toMap

        val low = get("low") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".low")
        }

        val high = get("high") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".high")
        }

        val entries = get("entries") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".entries")
        }

        val valuesFactory = get("values:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".values:type")
        }
        val values = get("values") match {
          case JsonArray(sub @ _*) => sub.map(valuesFactory.fromJsonFragment(_))
          case x => throw new JsonFormatException(x, name + ".values")
        }

        val underflowFactory = get("underflow:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".underflow:type")
        }
        val underflow = underflowFactory.fromJsonFragment(get("underflow"))

        val overflowFactory = get("overflow:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".overflow:type")
        }
        val overflow = overflowFactory.fromJsonFragment(get("overflow"))

        val nanflowFactory = get("nanflow:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".nanflow:type")
        }
        val nanflow = nanflowFactory.fromJsonFragment(get("nanflow"))

        new Binned[Container[_], Container[_], Container[_], Container[_]](low, high, entries, values, underflow, overflow, nanflow)

      case _ => throw new JsonFormatException(json, name)
    }
  }

  /** An accumulated quantity that was split into equally spaced bins between specified limits and filling only one bin per datum.
    * 
    * Use the factory [[org.dianahep.histogrammar.Bin]] to construct an instance.
    * 
    * @param low Minimum-value edge of the first bin.
    * @param high Maximum-value edge of the last bin.
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param values Containers for data sent to each bin.
    * @param underflow Container for data below the first bin.
    * @param overflow Container for data above the last bin.
    * @param nanflow Container for data that resulted in `NaN`.
    */
  class Binned[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]] private[histogrammar](
    val low: Double,
    val high: Double,
    val entries: Double,
    val values: Seq[V],
    val underflow: U,
    val overflow: O,
    val nanflow: N) extends Container[Binned[V, U, O, N]] with Bin.Methods {

    type Type = Binned[V, U, O, N]
    def factory = Bin

    if (low >= high)
      throw new ContainerException(s"low ($low) must be less than high ($high)")
    if (values.size < 1)
      throw new ContainerException(s"values ($values) must have at least one element")
    if (entries < 0.0)
      throw new ContainerException(s"entries ($entries) cannot be negative")
    def num = values.size

    /** Extract the container at a given index. */
    def at(index: Int) = values(index)

    def zero = new Binned[V, U, O, N](low, high, 0.0, Seq.fill(values.size)(values.head.zero), underflow.zero, overflow.zero, nanflow.zero)
    def +(that: Binned[V, U, O, N]): Binned[V, U, O, N] = {
      if (this.low != that.low)
        throw new ContainerException(s"cannot add Binned because low differs (${this.low} vs ${that.low})")
      if (this.high != that.high)
        throw new ContainerException(s"cannot add Binned because high differs (${this.high} vs ${that.high})")
      if (this.values.size != that.values.size)
        throw new ContainerException(s"cannot add Binned because number of values differs (${this.values.size} vs ${that.values.size})")
      if (this.values.isEmpty)
        throw new ContainerException(s"cannot add Binned because number of values is zero")

      new Binned(
        low,
        high,
        this.entries + that.entries,
        this.values zip that.values map {case (me, you) => me + you},
        this.underflow + that.underflow,
        this.overflow + that.overflow,
        this.nanflow + that.nanflow)
    }

    def toJsonFragment = JsonObject(
      "low" -> JsonFloat(low),
      "high" -> JsonFloat(high),
      "entries" -> JsonFloat(entries),
      "values:type" -> JsonString(values.head.factory.name),
      "values" -> JsonArray(values.map(_.toJsonFragment): _*),
      "underflow:type" -> JsonString(underflow.factory.name),
      "underflow" -> underflow.toJsonFragment,
      "overflow:type" -> JsonString(overflow.factory.name),
      "overflow" -> overflow.toJsonFragment,
      "nanflow:type" -> JsonString(nanflow.factory.name),
      "nanflow" -> nanflow.toJsonFragment)

    override def toString() = s"Binned[low=$low, high=$high, values=[${values.head.toString}..., size=${values.size}], underflow=$underflow, overflow=$overflow, nanflow=$nanflow]"
    override def equals(that: Any) = that match {
      case that: Binned[V, U, O, N] => this.low === that.low  &&  this.high === that.high  &&  this.entries === that.entries  &&  this.values == that.values  &&  this.underflow == that.underflow  &&  this.overflow == that.overflow  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (low, high, entries, values, underflow, overflow, nanflow).hashCode
  }

  /** Accumulating a quantity by splitting it into equally spaced bins between specified limits and filling only one bin per datum.
    * 
    * Use the factory [[org.dianahep.histogrammar.Bin]] to construct an instance.
    * 
    * @param low Minimum-value edge of the first bin.
    * @param high Maximum-value edge of the last bin.
    * @param quantity Numerical function to track.
    * @param selection Boolean or non-negative function that cuts or weights entries.
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param values Containers for data sent to each bin.
    * @param underflow Container for data below the first bin.
    * @param overflow Container for data above the last bin.
    * @param nanflow Container for data that resulted in `NaN`.
    */
  class Binning[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, U <: Container[U] with Aggregation{type Datum >: DATUM}, O <: Container[O] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}] private[histogrammar](
    val low: Double,
    val high: Double,
    val quantity: NumericalFcn[DATUM],
    val selection: Selection[DATUM],
    var entries: Double,
    val values: Seq[V],
    val underflow: U,
    val overflow: O,
    val nanflow: N) extends Container[Binning[DATUM, V, U, O, N]] with AggregationOnData with Bin.Methods {

    type Type = Binning[DATUM, V, U, O, N]
    type Datum = DATUM
    def factory = Bin

    if (low >= high)
      throw new ContainerException(s"low ($low) must be less than high ($high)")
    if (values.size < 1)
      throw new ContainerException(s"values ($values) must have at least one element")
    if (entries < 0.0)
      throw new ContainerException(s"entries ($entries) cannot be negative")
    def num = values.size

    /** Extract the container at a given index. */
    def at(index: Int) = values(index)

    def zero = new Binning[DATUM, V, U, O, N](low, high, quantity, selection, 0.0, Seq.fill(values.size)(values.head.zero), underflow.zero, overflow.zero, nanflow.zero)
    def +(that: Binning[DATUM, V, U, O, N]): Binning[DATUM, V, U, O, N] = {
      if (this.low != that.low)
        throw new ContainerException(s"cannot add Binning because low differs (${this.low} vs ${that.low})")
      if (this.high != that.high)
        throw new ContainerException(s"cannot add Binning because high differs (${this.high} vs ${that.high})")
      if (this.values.size != that.values.size)
        throw new ContainerException(s"cannot add Binning because number of values differs (${this.values.size} vs ${that.values.size})")
      if (this.values.isEmpty)
        throw new ContainerException(s"cannot add Binning because number of values is zero")
      if (this.values.head.factory != that.values.head.factory)
        throw new ContainerException(s"cannot add Binning because values type differs (${this.values.head.factory.name} vs ${that.values.head.factory.name})")
      if (this.underflow.factory != that.underflow.factory)
        throw new ContainerException(s"cannot add Binning because underflow type differs (${this.underflow.factory.name} vs ${that.underflow.factory.name})")
      if (this.overflow.factory != that.overflow.factory)
        throw new ContainerException(s"cannot add Binning because overflow type differs (${this.overflow.factory.name} vs ${that.overflow.factory.name})")
      if (this.nanflow.factory != that.nanflow.factory)
        throw new ContainerException(s"cannot add Binning because nanflow type differs (${this.nanflow.factory.name} vs ${that.nanflow.factory.name})")

      new Binning[DATUM, V, U, O, N](
        low,
        high,
        this.quantity,
        this.selection,
        this.entries + that.entries,
        this.values zip that.values map {case (me, you) => me + you},
        this.underflow + that.underflow,
        this.overflow + that.overflow,
        this.nanflow + that.nanflow)
    }

    def fill[SUB <: Datum](datum: SUB, weight: Double = 1.0) {
      val w = weight * selection(datum)
      if (w > 0.0) {
        val q = quantity(datum)

        entries += w
        if (under(q))
          underflow.fill(datum, w)
        else if (over(q))
          overflow.fill(datum, w)
        else if (nan(q))
          nanflow.fill(datum, w)
        else
          values(bin(q)).fill(datum, w)
      }
    }

    def toJsonFragment = JsonObject(
      "low" -> JsonFloat(low),
      "high" -> JsonFloat(high),
      "entries" -> JsonFloat(entries),
      "values:type" -> JsonString(values.head.factory.name),
      "values" -> JsonArray(values.map(_.toJsonFragment): _*),
      "underflow:type" -> JsonString(underflow.factory.name),
      "underflow" -> underflow.toJsonFragment,
      "overflow:type" -> JsonString(overflow.factory.name),
      "overflow" -> overflow.toJsonFragment,
      "nanflow:type" -> JsonString(nanflow.factory.name),
      "nanflow" -> nanflow.toJsonFragment)

    override def toString() = s"Binning[low=$low, high=$high, values=[${values.head.toString}..., size=${values.size}], underflow=$underflow, overflow=$overflow, nanflow=$nanflow]"
    override def equals(that: Any) = that match {
      case that: Binning[DATUM, V, U, O, N] => this.low === that.low  &&  this.high === that.high  &&  this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.entries === that.entries  &&  this.values == that.values  &&  this.underflow == that.underflow  &&  this.overflow == that.overflow  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (low, high, quantity, selection, entries, values, underflow, overflow, nanflow).hashCode
  }
}

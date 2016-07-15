// Copyright 2016 DIANA-HEP
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

import scala.language.existentials

import org.dianahep.histogrammar.json._
import org.dianahep.histogrammar.util._

package histogrammar {
  //////////////////////////////////////////////////////////////// CentrallyBin/CentrallyBinned/CentrallyBinning

  /** Split a quantity into bins defined by irregularly spaced bin centers, with exactly one sub-aggregator filled per datum (the closest one).
    * 
    * Unlike irregular bins defined by explicit ranges, irregular bins defined by bin centers are guaranteed to fully partition the space with no gaps and no overlaps. It could be viewed as cluster scoring in one dimension.
    * 
    * Factory produces mutable [[org.dianahep.histogrammar.CentrallyBinning]] and immutable [[org.dianahep.histogrammar.CentrallyBinned]] objects.
    */
  object CentrallyBin extends Factory {
    val name = "CentrallyBin"
    val help = "Split a quantity into bins defined by irregularly spaced bin centers, with exactly one sub-aggregator filled per datum (the closest one)."
    val detailedHelp = """Unlike irregular bins defined by explicit ranges, irregular bins defined by bin centers are guaranteed to fully partition the space with no gaps and no overlaps. It could be viewed as cluster scoring in one dimension."""

    /** Create an immutable [[org.dianahep.histogrammar.CentrallyBinned]] from arguments (instead of JSON).
      * 
      * @param entries Weighted number of entries (sum of all observed weights).
      * @param bins Centers and values of each bin.
      * @param nanflow Container for data that resulted in `NaN`.
      */
    def ed[V <: Container[V] with NoAggregation, N <: Container[N] with NoAggregation](entries: Double, bins: Iterable[(Double, V)], nanflow: N) =
      new CentrallyBinned[V, N](entries, None, immutable.MetricSortedMap(bins.toSeq: _*), nanflow)

    /** Create an empty, mutable [[org.dianahep.histogrammar.CentrallyBinning]].
      * 
      * @param bins Centers of each bin.
      * @param quantity Numerical function split into fixed but unevenly-spaced bins.
      * @param value Template used to create zero values (by calling this `value`'s `zero` method).
      * @param nanflow Container for data that result in `NaN`.
      */
    def apply[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}]
      (bins: Iterable[Double], quantity: UserFcn[DATUM, Double], value: => V = Count(), nanflow: N = Count()) =
      new CentrallyBinning[DATUM, V, N](quantity, 0.0, value, mutable.MetricSortedMap(bins.toSeq.map((_, value.zero)): _*), nanflow)

    /** Synonym for `apply`. */
    def ing[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}]
      (bins: Iterable[Double], quantity: UserFcn[DATUM, Double], value: => V = Count(), nanflow: N = Count()) =
      apply(bins, quantity, value, nanflow)

    trait Methods[V <: Container[V]] {
      /**Weighted number of entries (sum of all observed weights). */
      def entries: Double
      /** Bin centers and their contents. */
      def bins: MetricSortedMap[Double, V]

      /** Set of centers of each bin. */
      def centersSet = bins.iterator.map(_._1).toSet
      /** Iterable over the centers of each bin. */
      def centers = bins.iterator.map(_._1)
      /** Iterable over the containers associated with each bin. */
      def values = bins.iterator.map(_._2)

      /** Return the exact center of the bin that `x` belongs to. */
      def center(x: Double): Double = bins.closest(x).get.key
      /** Return the aggregator at the center of the bin that `x` belongs to. */
      def value(x: Double): V = bins.closest(x).get.value
      /** Return `true` iff `x` is in the nanflow region (equal to `NaN`). */
      def nan(x: Double): Boolean = x.isNaN

      /** Find the lower and upper neighbors of a bin (given by exact bin center). */
      def neighbors(center: Double): (Option[Double], Option[Double]) = {
        if (!bins.contains(center))
          throw new IllegalArgumentException(s"position $center is not the exact center of a bin")
        (bins.closest(Math.nextAfter(center, java.lang.Double.NEGATIVE_INFINITY), Some((x: Double, v: V) => x < center)).map(_.key), bins.closest(Math.nextAfter(center, java.lang.Double.POSITIVE_INFINITY), Some((x: Double, v: V) => x > center)).map(_.key))
      }

      /** Get the low and high edge of a bin (given by exact bin center). */
      def range(center: Double): (Double, Double) = neighbors(center) match {
        case (Some(below), Some(above)) => ((below + center)/2.0, (center + above)/2.0)
        case (None, Some(above)) => (java.lang.Double.NEGATIVE_INFINITY, (center + above)/2.0)
        case (Some(below), None) => ((below + center)/2.0, java.lang.Double.POSITIVE_INFINITY)
        case _ => throw new Exception("can't get here")
      }
    }

    import KeySetComparisons._
    def fromJsonFragment(json: Json, nameFromParent: Option[String]): Container[_] with NoAggregation = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet has Set("entries", "bins:type", "bins", "nanflow:type", "nanflow").maybe("name").maybe("bins:name")) =>
        val get = pairs.toMap

        val entries = get("entries") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".entries")
        }

        val quantityName = get.getOrElse("name", JsonNull) match {
          case JsonString(x) => Some(x)
          case JsonNull => None
          case x => throw new JsonFormatException(x, name + ".name")
        }

        val binsFactory = get("bins:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".bins:type")
        }
        val binsName = get.getOrElse("bins:name", JsonNull) match {
          case JsonString(x) => Some(x)
          case JsonNull => None
          case x => throw new JsonFormatException(x, name + ".bins:name")
        }
        val bins = get("bins") match {
          case JsonArray(bins @ _*) if (bins.size >= 2) =>
            immutable.MetricSortedMap(bins.zipWithIndex map {
              case (JsonObject(binpair @ _*), i) if (binpair.keySet has Set("center", "value")) =>
                val binget = binpair.toMap

                val center = binget("center") match {
                  case JsonNumber(x) => x
                  case x => throw new JsonFormatException(x, name + s".bins $i center")
                }

                val value = binsFactory.fromJsonFragment(binget("value"), binsName)
                (center, value)

              case (x, i) => throw new JsonFormatException(x, name + s".bins $i")
            }: _*)

          case x => throw new JsonFormatException(x, name + ".bins")
        }

        val nanflowFactory = get("nanflow:type") match {
          case JsonString(name) => Factory(name)
          case x => throw new JsonFormatException(x, name + ".nanflow:type")
        }
        val nanflow = nanflowFactory.fromJsonFragment(get("nanflow"), None)

        new CentrallyBinned[Container[_], Container[_]](entries, (nameFromParent ++ quantityName).lastOption, bins.asInstanceOf[immutable.MetricSortedMap[Double, Container[_]]], nanflow)

      case _ => throw new JsonFormatException(json, name)
    }
  }

  /** An accumulated quantity that was split into bins defined by bin centers, filling only one datum per bin with no overflows or underflows.
    * 
    * Use the factory [[org.dianahep.histogrammar.CentrallyBin]] to construct an instance.
    * 
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param quantityName Optional name given to the quantity function, passed for bookkeeping.
    * @param bins Metric, sorted map of centers and values for each bin.
    * @param nanflow Container for data that resulted in `NaN`.
    */
  class CentrallyBinned[V <: Container[V] with NoAggregation, N <: Container[N] with NoAggregation] private[histogrammar](val entries: Double, val quantityName: Option[String], val bins: immutable.MetricSortedMap[Double, V], val nanflow: N)
    extends Container[CentrallyBinned[V, N]] with NoAggregation with QuantityName with CentrallyBin.Methods[V] {

    type Type = CentrallyBinned[V, N]
    type EdType = CentrallyBinned[V, N]
    def factory = CentrallyBin

    if (entries < 0.0)
      throw new ContainerException(s"entries ($entries) cannot be negative")
    if (bins.size < 2)
      throw new ContainerException(s"number of bins (${bins.size}) must be at least two")

    def zero = new CentrallyBinned[V, N](0.0, quantityName, immutable.MetricSortedMap[Double, V](bins.toSeq.map({case (c, v) => (c, v.zero)}): _*), nanflow.zero)
    def +(that: CentrallyBinned[V, N]) = {
      if (this.quantityName != that.quantityName)
        throw new ContainerException(s"cannot add ${getClass.getName} because quantityName differs (${this.quantityName} vs ${that.quantityName})")
      if (this.centers != that.centers)
        throw new ContainerException(s"cannot add ${getClass.getName} because centers are different:\n    ${this.centers}\nvs\n    ${that.centers}")

      val newbins = immutable.MetricSortedMap(this.bins.toSeq zip that.bins.toSeq map {case ((c1, v1), (_, v2)) => (c1, v1 + v2)}: _*)
      
      new CentrallyBinned[V, N](this.entries + that.entries, quantityName, newbins, this.nanflow + that.nanflow)
    }

    def children = nanflow :: values.toList

    def toJsonFragment(suppressName: Boolean) = JsonObject(
      "entries" -> JsonFloat(entries),
      "bins:type" -> JsonString(bins.head._2.factory.name),
      "bins" -> JsonArray(bins.toSeq map {case (c, v) => JsonObject("center" -> JsonFloat(c), "value" -> v.toJsonFragment(true))}: _*),
      "nanflow:type" -> JsonString(nanflow.factory.name),
      "nanflow" -> nanflow.toJsonFragment(false)).
      maybe(JsonString("name") -> (if (suppressName) None else quantityName.map(JsonString(_)))).
      maybe(JsonString("bins:name") -> (bins.head match {case (c, v: QuantityName) => v.quantityName.map(JsonString(_)); case _ => None}))

    override def toString() = s"""<CentrallyBinned bins=${bins.head._2.factory.name} size=${bins.size} nanflow=${nanflow.factory.name}>"""
    override def equals(that: Any) = that match {
      case that: CentrallyBinned[V, N] => this.entries === that.entries  &&  this.quantityName == that.quantityName  &&  this.bins == that.bins  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (entries, quantityName, bins, nanflow).hashCode
  }

  /** Accumulating a quantity by splitting it into bins defined by bin centers, filling only one datum per bin with no overflows or underflows.
    * 
    * Use the factory [[org.dianahep.histogrammar.CentrallyBin]] to construct an instance.
    * 
    * @param quantity Numerical function to track.
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param value New value (note the `=>`: expression is reevaluated every time a new value is needed).
    * @param bins Metric, sorted map of centers and values for each bin.
    * @param nanflow Container for data that resulted in `NaN`.
    */
  class CentrallyBinning[DATUM, V <: Container[V] with Aggregation{type Datum >: DATUM}, N <: Container[N] with Aggregation{type Datum >: DATUM}] private[histogrammar]
    (val quantity: UserFcn[DATUM, Double], var entries: Double, value: => V, val bins: mutable.MetricSortedMap[Double, V], val nanflow: N)
    extends Container[CentrallyBinning[DATUM, V, N]] with AggregationOnData with NumericalQuantity[DATUM] with CentrallyBin.Methods[V] {

    protected val v = value
    type Type = CentrallyBinning[DATUM, V, N]
    type EdType = CentrallyBinned[v.EdType, nanflow.EdType]
    type Datum = DATUM
    def factory = CentrallyBin

    if (entries < 0.0)
      throw new ContainerException(s"entries ($entries) cannot be negative")
    if (bins.size < 2)
      throw new ContainerException(s"number of bins (${bins.size}) must be at least two")

    def zero = new CentrallyBinning[DATUM, V, N](quantity, 0.0, value, mutable.MetricSortedMap[Double, V](bins.toSeq.map({case (c, v) => (c, v.zero)}): _*), nanflow.zero)
    def +(that: CentrallyBinning[DATUM, V, N]) = {
      if (this.quantity.name != that.quantity.name)
        throw new ContainerException(s"cannot add ${getClass.getName} because quantity name differs (${this.quantity.name} vs ${that.quantity.name})")
      if (this.centers != that.centers)
        throw new ContainerException(s"cannot add ${getClass.getName} because centers are different:\n    ${this.centers}\nvs\n    ${that.centers}")

      val newbins = mutable.MetricSortedMap(this.bins.toSeq zip that.bins.toSeq map {case ((c1, v1), (_, v2)) => (c1, v1 + v2)}: _*)

      new CentrallyBinning[DATUM, V, N](quantity, this.entries + that.entries, value, newbins, this.nanflow + that.nanflow)
    }

    def fill[SUB <: Datum](datum: SUB, weight: Double = 1.0) {
      checkForCrossReferences()
      if (weight > 0.0) {
        val q = quantity(datum)

        if (nan(q))
          nanflow.fill(datum, weight)
        else {
          val Some(Closest(_, _, value)) = bins.closest(q)
          value.fill(datum, weight)
        }

        // no possibility of exception from here on out (for rollback)
        entries += weight
      }
    }

    def children = nanflow :: values.toList

    def toJsonFragment(suppressName: Boolean) = JsonObject(
      "entries" -> JsonFloat(entries),
      "bins:type" -> JsonString(value.factory.name),
      "bins" -> JsonArray(bins.toSeq map {case (c, v) => JsonObject("center" -> JsonFloat(c), "value" -> v.toJsonFragment(true))}: _*),
      "nanflow:type" -> JsonString(nanflow.factory.name),
      "nanflow" -> nanflow.toJsonFragment(false)).
      maybe(JsonString("name") -> (if (suppressName) None else quantity.name.map(JsonString(_)))).
      maybe(JsonString("bins:name") -> (bins.head match {case (c, v: AnyQuantity[_, _]) => v.quantity.name.map(JsonString(_)); case _ => None}))

    override def toString() = s"""<CentrallyBinning bins=${bins.head._2.factory.name} size=${bins.size} nanflow=${nanflow.factory.name}>"""
    override def equals(that: Any) = that match {
      case that: CentrallyBinning[DATUM, V, N] => this.quantity == that.quantity  &&  this.entries === that.entries  &&  this.bins == that.bins  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (quantity, entries, bins, nanflow).hashCode
  }
}

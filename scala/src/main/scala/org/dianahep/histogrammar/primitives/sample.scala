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
import org.dianahep.histogrammar.util._

package histogrammar {
  //////////////////////////////////////////////////////////////// Sample

  /** Accumulate raw numbers, vectors of numbers, or strings, merging identical values.
    * 
    * Factory produces mutable [[org.dianahep.histogrammar.Sampling]] and immutable [[org.dianahep.histogrammar.Sampled]] objects.
    */
  object Sample extends Factory {
    val name = "Sample"
    val help = "Accumulate raw numbers, vectors of numbers, or strings that are an unbiased sample of the observed distribution."
    val detailedHelp = "Sample(limit: Int, quantity: UserFcn[DATUM, RANGE], selection: Selection[DATUM] = unweighted[DATUM])"

    /** Create an immutable [[org.dianahep.histogrammar.Sampled]] from arguments (instead of JSON).
      * 
      * @param entries Weighted number of entries (sum of all observed weights).
      * @param limit Maximum number of data points in the sample.
      * @param values Distinct multidimensional vectors and their weights, sampled from the observed distribution.
      */
    def ed[RANGE](entries: Double, limit: Int, values: (RANGE, Double)*) =
      new Sampled(entries, limit, values: _*)

    /** Create an empty, mutable [[org.dianahep.histogrammar.Sampling]].
      * 
      * @param limit Maximum number of data points in the sample.
      * @param quantity Function that produces numbers, vectors of numbers, or strings.
      * @param selection Boolean or non-negative function that cuts or weights entries.
      */
    def apply[DATUM, RANGE](limit: Int, quantity: UserFcn[DATUM, RANGE], selection: Selection[DATUM] = unweighted[DATUM]) =
      new Sampling[DATUM, RANGE](quantity, selection, 0.0, new mutable.Reservoir[RANGE](limit))

    /** Synonym for `apply`. */
    def ing[DATUM, RANGE](limit: Int, quantity: UserFcn[DATUM, RANGE], selection: Selection[DATUM] = unweighted[DATUM]) = apply(limit, quantity, selection)

    /** Use [[org.dianahep.histogrammar.Sampled]] in Scala pattern-matching. */
    def unapply[RANGE](x: Sampled[RANGE]) = x.values
    /** Use [[org.dianahep.histogrammar.Sampling]] in Scala pattern-matching. */
    def unapply[DATUM, RANGE](x: Sampling[DATUM, RANGE]) = x.values

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("entries", "limit", "values")) =>
        val get = pairs.toMap

        val entries = get("entries") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".entries")
        }

        val limit = get("limit") match {
          case JsonInt(x) => x
          case x => throw new JsonFormatException(x, name + ".limit")
        }

        val values = get("values") match {
          case JsonArray(elems @ _*) => Seq[(Any, Double)](elems.zipWithIndex map {
            case (JsonObject(wv @ _*), i) if (wv.keySet == Set("w", "v")) =>
              val wvget = wv.toMap

              val n = wvget("w") match {
                case JsonNumber(x) => x
                case x => throw new JsonFormatException(x, name + s".values $i n")
              }

              val v: Any = wvget("v") match {
                case JsonString(x) => x
                case JsonNumber(x) => x
                case JsonArray(d @ _*) => Vector(d.zipWithIndex map {
                  case (JsonNumber(x), j) => x
                  case (x, j) => throw new JsonFormatException(x, name + s".values $i v $j")
                }: _*)
                case x => throw new JsonFormatException(x, name + s".values $i v")
              }

              (v -> n)

            case (x, i) => throw new JsonFormatException(x, name + s".values $i")
          }: _*)
          case x => throw new JsonFormatException(x, name + ".values")
        }

        new Sampled[Any](entries, limit.toInt, values: _*)

      case _ => throw new JsonFormatException(json, name)
    }
  }

  /** An accumulated sample of numbers, vectors of numbers, or strings.
    * 
    * Use the factory [[org.dianahep.histogrammar.Sample]] to construct an instance.
    * 
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param limit Maximum number of data points in the sample.
    * @param values Distinct multidimensional vectors and their weights, sampled from the observed distribution.
    */
  class Sampled[RANGE] private[histogrammar](val entries: Double, val limit: Int, val values: (RANGE, Double)*) extends Container[Sampled[RANGE]] {
    type Type = Sampled[RANGE]
    def factory = Sample

    if (limit <= 0)
      throw new ContainerException(s"limit ($limit) must be positive")

    /** Number of data points in the sample (saturates at `limit`). */
    def size = values.size
    /** Determine if the sample is empty. */
    def isEmpty = values.isEmpty

    def zero = new Sampled(0.0, limit)
    def +(that: Sampled[RANGE]) = {
      if (this.limit != that.limit)
        throw new ContainerException(s"cannot add Sampled because limit differs (${this.limit} vs ${that.limit})")

      val reservoir = new mutable.Reservoir[RANGE](limit, values: _*)
      that.values foreach {case (y, weight) => reservoir.update(y, weight)}

      new Sampled[RANGE](this.entries + that.entries, this.limit, reservoir.values: _*)
    }

    def toJsonFragment = {
      implicit val rangeOrdering = Bag.rangeOrdering[RANGE]
      JsonObject(
        "entries" -> JsonFloat(entries),
        "limit" -> JsonInt(limit),
        "values" -> JsonArray(values.toSeq.sortBy(_._1).map({
          case (v: String, n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonString(v))
          case (v: Double, n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonFloat(v))
          case (v: Vector[_], n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonArray(v.map({case vi: Double => JsonFloat(vi)}): _*))
        }): _*))
    }

    override def toString() = s"""Sampled[${if (isEmpty) "empty" else values.head.toString + "..."}, size=${size}]"""
    override def equals(that: Any) = that match {
      case that: Sampled[RANGE] => this.entries === that.entries  &&  this.limit == that.limit  &&  this.values == that.values
      case _ => false
    }
    override def hashCode() = (entries, limit, values).hashCode()
  }

  /** An accumulated sample of numbers, vectors of numbers, or strings.
    * 
    * Use the factory [[org.dianahep.histogrammar.Sample]] to construct an instance.
    * 
    * @param quantity Function that produces numbers, vectors of numbers, or strings.
    * @param selection Boolean or non-negative function that cuts or weights entries.
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param reservoir Data structure to perform weighted reservoir sampling.
    */
  class Sampling[DATUM, RANGE] private[histogrammar](val quantity: UserFcn[DATUM, RANGE], val selection: Selection[DATUM], var entries: Double, reservoir: mutable.Reservoir[RANGE]) extends Container[Sampling[DATUM, RANGE]] with AggregationOnData {
    type Type = Sampling[DATUM, RANGE]
    type Datum = DATUM
    def factory = Sample

    if (limit <= 0)
      throw new ContainerException(s"limit ($limit) must be positive")

    /** Maximum number of data points in the sample. */
    def limit = reservoir.limit
    /** Distinct multidimensional vectors and their weights, sampled from the observed distribution. */
    def values = reservoir.values
    /** Number of data points in the sample (saturates at `limit`). */
    def size = reservoir.size
    /** Determine if the sample is empty. */
    def isEmpty = reservoir.isEmpty

    def zero = new Sampling(quantity, selection, 0.0, new mutable.Reservoir[RANGE](limit))
    def +(that: Sampling[DATUM, RANGE]) = {
      if (this.limit != that.limit)
        throw new ContainerException(s"cannot add Sampling because limit differs (${this.limit} vs ${that.limit})")

      val newreservoir = new mutable.Reservoir[RANGE](this.limit, this.values: _*)
      that.values foreach {case (y, weight) => newreservoir.update(y, weight)}

      new Sampling[DATUM, RANGE](quantity, selection, this.entries + that.entries, newreservoir)
    }

    def fill[SUB <: Datum](datum: SUB, weight: Double = 1.0) {
      val w = weight * selection(datum)
      if (w > 0.0) {
        val q = quantity(datum)
        entries += w
        reservoir.update(q, w)
      }
    }

    def toJsonFragment = {
      implicit val rangeOrdering = Bag.rangeOrdering[RANGE]
      JsonObject(
        "entries" -> JsonFloat(entries),
        "limit" -> JsonInt(limit),
        "values" -> JsonArray(values.toSeq.sortBy(_._1).map({
          case (v: String, n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonString(v))
          case (v: Double, n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonFloat(v))
          case (v: Vector[_], n) => JsonObject("w" -> JsonFloat(n), "v" -> JsonArray(v.map({case vi: Double => JsonFloat(vi)}): _*))
        }): _*))
    }

    override def toString() = s"""Sampling[${if (isEmpty) "empty" else reservoir.some.toString + "..."}, size=${size}]"""

    override def equals(that: Any) = that match {
      case that: Sampling[DATUM, RANGE] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.entries === that.entries  &&  this.limit == that.limit  &&  this.values == that.values
      case _ => false
    }

    override def hashCode() = (quantity, selection, entries, limit, values).hashCode()
  }
}

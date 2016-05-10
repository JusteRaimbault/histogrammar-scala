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
  //////////////////////////////////////////////////////////////// Bag/Bagged/Bagging

  /**Accumulate raw data up to an optional limit, at which point only the total number is preserved.
    * 
    * Factory produces mutable [[org.dianahep.histogrammar.Bagging]] and immutable [[org.dianahep.histogrammar.Bagged]] objects.
    */
  object Bag extends Factory {
    val name = "Bag"
    val help = "Accumulate raw data up to an optional limit, at which point only the total number is preserved."
    val detailedHelp = """Bag(quantity: MultivariateFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM], limit: Option[Double] = None)"""

    /** Create an immutable [[org.dianahep.histogrammar.Bagged]] from arguments (instead of JSON).
      * 
      * @param entries Weighted number of entries (sum of all observed weights).
      * @param limit If not `None` and `entries > limit`, the `values` are dropped, leaving only `entries` to count data.
      * @param values Distinct multidimensional vectors and the (weighted) number of times they were observed or `None` if they were dropped.
      */
    def ed[RANGE](entries: Double, limit: Option[Double], values: Option[Map[RANGE, Double]]) =
      new Bagged[RANGE](entries, limit, values)

    /** Create an empty, mutable [[org.dianahep.histogrammar.Bagging]].
      * 
      * @param quantity Multivariate function to track.
      * @param selection Boolean or non-negative function that cuts or weights entries.
      * @param limit If not `None` and `entries > limit`, the `values` are dropped, leaving only `entries` to count data.
      */
    def apply[DATUM, RANGE](quantity: UserFcn[DATUM, RANGE], selection: Selection[DATUM] = unweighted[DATUM], limit: Option[Double] = None) =
      new Bagging[DATUM, RANGE](quantity, selection, limit, 0.0, Some(scala.collection.mutable.Map[RANGE, Double]()))

    /** Synonym for `apply`. */
    def ing[DATUM, RANGE](quantity: UserFcn[DATUM, RANGE], selection: Selection[DATUM] = unweighted[DATUM], limit: Option[Double] = None) =
      apply(quantity, selection, limit)

    /** Use [[org.dianahep.histogrammar.Bagged]] in Scala pattern-matching. */
    def unapply[RANGE](x: Bagged[RANGE]) = x.values
    /** Use [[org.dianahep.histogrammar.Bagging]] in Scala pattern-matching. */
    def unapply[DATUM, RANGE](x: Bagging[DATUM, RANGE]) = x.values

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("entries", "limit", "values")) =>
        val get = pairs.toMap

        val entries = get("entries") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, name + ".entries")
        }

        val limit = get("limit") match {
          case JsonNumber(x) => Some(x)
          case JsonNull => None
          case x => throw new JsonFormatException(x, name + ".limit")
        }

        val values = get("values") match {
          case JsonArray(elems @ _*) => Some(Map[Any, Double](elems.zipWithIndex map {
            case (JsonObject(nv @ _*), i) if (nv.keySet == Set("n", "v")) =>
              val nvget = nv.toMap

              val n = nvget("n") match {
                case JsonNumber(x) => x
                case x => throw new JsonFormatException(x, name + s".values $i n")
              }

              val v: Any = nvget("v") match {
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
          }: _*))
          case JsonNull => None
          case x => throw new JsonFormatException(x, name + ".values")
        }

        new Bagged[Any](entries, limit, values)

      case _ => throw new JsonFormatException(json, name)
    }
  }

  /** An accumulated set of raw data or just the number of entries if it exceeded its limit.
    * 
    * Use the factory [[org.dianahep.histogrammar.Bag]] to construct an instance.
    * 
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param limit If not `None` and `entries > limit`, the `values` are dropped, leaving only `entries` to count data.
    * @param values Distinct multidimensional vectors and the (weighted) number of times they were observed or `None` if they were dropped.
    */
  class Bagged[RANGE] private[histogrammar](val entries: Double, val limit: Option[Double], val values: Option[Map[RANGE, Double]]) extends Container[Bagged[RANGE]] {
    type Type = Bagged[RANGE]
    def factory = Bag

    def zero = new Bagged(0.0, limit, Some(Map[RANGE, Double]()))
    def +(that: Bagged[RANGE]) = {
      val newentries = this.entries + that.entries
      val newvalues =
        if (!limit.isEmpty  &&  limit.get < newentries)
          None
        else if (that.values.isEmpty)
          this.values
        else if (this.values.isEmpty)
          that.values
        else {
          val out = scala.collection.mutable.Map(this.values.get.toSeq: _*)
          that.values.get foreach {case (k, v) =>
            if (out contains k)
              out(k) += v
            else
              out(k) = v
          }
          Some(out.toMap)
        }

      new Bagged[RANGE](newentries, limit, newvalues)
    }

    def toJsonFragment = {
      implicit val rangeOrdering = new Ordering[RANGE] {
        def compare(x: RANGE, y: RANGE) = (x, y) match {
          case (xx: String, yy: String) => xx compare yy
          case (xx: Double, yy: Double) => xx compare yy
          case (xx: Vector[_], yy: Vector[_]) if (!xx.isEmpty  &&  !yy.isEmpty  &&  xx.head == yy.head) => compare(xx.tail.asInstanceOf[RANGE], yy.tail.asInstanceOf[RANGE])
          case (xx: Vector[_], yy: Vector[_]) if (!xx.isEmpty  &&  !yy.isEmpty) => xx.head.asInstanceOf[Double] compare yy.head.asInstanceOf[Double]
          case (xx: Vector[_], yy: Vector[_]) if (xx.isEmpty  &&  yy.isEmpty) => 0
          case (xx: Vector[_], yy: Vector[_]) if (xx.isEmpty) => -1
          case (xx: Vector[_], yy: Vector[_]) if (yy.isEmpty) => 1
        }
      }

      JsonObject(
        "entries" -> JsonFloat(entries),
        "limit" -> (limit match {
          case Some(x) => JsonFloat(x)
          case None => JsonNull
        }),
        "values" -> (values match {
          case Some(m) => JsonArray(m.toSeq.sortBy(_._1).map({
            case (v: String, n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonString(v))
            case (v: Double, n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonFloat(v))
            case (v: Vector[_], n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonArray(v.map({case vi: Double => JsonFloat(vi)}): _*))
          }): _*)
          case None => JsonNull
        }))
    }

    override def toString() = s"""Bagged[${if (!limit.isEmpty  &&  limit.get < entries) "saturated" else "size=" + values.map(_.size).toList.sum.toString}]"""
    override def equals(that: Any) = that match {
      case that: Bagged[RANGE] => this.entries === that.entries  &&  this.limit == that.limit  &&  this.values == that.values
      case _ => false
    }
    override def hashCode() = (entries, limit, values).hashCode()
  }

  /** Accumulating a quantity as raw data up to an optional limit, at which point only the total number are preserved.
    * 
    * Use the factory [[org.dianahep.histogrammar.Bag]] to construct an instance.
    * 
    * @param quantity Multivariate function to track.
    * @param selection Boolean or non-negative function that cuts or weights entries.
    * @param limit If not `None` and `entries > limit`, the `values` are dropped, leaving only `entries` to count data.
    * @param entries Weighted number of entries (sum of all observed weights).
    * @param values Distinct multidimensional vectors and the (weighted) number of times they were observed or `None` if they were dropped.
    */
  class Bagging[DATUM, RANGE] private[histogrammar](val quantity: UserFcn[DATUM, RANGE], val selection: Selection[DATUM], val limit: Option[Double], var entries: Double, var values: Option[scala.collection.mutable.Map[RANGE, Double]]) extends Container[Bagging[DATUM, RANGE]] with AggregationOnData {
    type Type = Bagging[DATUM, RANGE]
    type Datum = DATUM
    def factory = Bag

    def zero = new Bagging[DATUM, RANGE](quantity, selection, limit, 0.0, Some(scala.collection.mutable.Map[RANGE, Double]()))
    def +(that: Bagging[DATUM, RANGE]) = {
      val newentries = this.entries + that.entries
      val newvalues =
        if (!limit.isEmpty  &&  limit.get < newentries)
          None
        else if (that.values.isEmpty)
          this.values
        else if (this.values.isEmpty)
          that.values
        else {
          val out = scala.collection.mutable.Map(this.values.get.toSeq: _*)
          that.values.get foreach {case (k, v) =>
            if (out contains k)
              out(k) += v
            else
              out(k) = v
          }
          Some(out)
        }

      new Bagging[DATUM, RANGE](quantity, selection, limit, newentries, newvalues)
    }

    def fill[SUB <: Datum](datum: SUB, weight: Double = 1.0) {
      val w = weight * selection(datum)
      if (w > 0.0) {
        val q = quantity(datum)
        entries += w
        if (!limit.isEmpty  &&  limit.get < entries)
          values = None
        else
          if (!values.isEmpty) {
            if (values.get contains q)
              values.get(q) += w
            else
              values.get(q) = w
          }
      }
    }

    def toJsonFragment = {
      implicit val rangeOrdering = new Ordering[RANGE] {
        def compare(x: RANGE, y: RANGE) = (x, y) match {
          case (xx: String, yy: String) => xx compare yy
          case (xx: Double, yy: Double) => xx compare yy
          case (xx: Vector[_], yy: Vector[_]) if (!xx.isEmpty  &&  !yy.isEmpty  &&  xx.head == yy.head) => compare(xx.tail.asInstanceOf[RANGE], yy.tail.asInstanceOf[RANGE])
          case (xx: Vector[_], yy: Vector[_]) if (!xx.isEmpty  &&  !yy.isEmpty) => xx.head.asInstanceOf[Double] compare yy.head.asInstanceOf[Double]
          case (xx: Vector[_], yy: Vector[_]) if (xx.isEmpty  &&  yy.isEmpty) => 0
          case (xx: Vector[_], yy: Vector[_]) if (xx.isEmpty) => -1
          case (xx: Vector[_], yy: Vector[_]) if (yy.isEmpty) => 1
        }
      }

      JsonObject(
        "entries" -> JsonFloat(entries),
        "limit" -> (limit match {
          case Some(x) => JsonFloat(x)
          case None => JsonNull
        }),
        "values" -> (values match {
          case Some(m) => JsonArray(m.toSeq.sortBy(_._1).map({
            case (v: String, n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonString(v))
            case (v: Double, n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonFloat(v))
            case (v: Vector[_], n) => JsonObject("n" -> JsonFloat(n), "v" -> JsonArray(v.map({case vi: Double => JsonFloat(vi)}): _*))
          }): _*)
          case None => JsonNull
        }))
    }

    override def toString() = s"""Bagging[${if (!limit.isEmpty  &&  limit.get < entries) "saturated" else "size=" + values.map(_.size).toList.sum.toString}]"""
    override def equals(that: Any) = that match {
      case that: Bagging[DATUM, RANGE] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.entries === that.entries  &&  this.limit == that.limit  &&  this.values == that.values
      case _ => false
    }
    override def hashCode() = (quantity, selection, entries, limit, values).hashCode()
  }

}

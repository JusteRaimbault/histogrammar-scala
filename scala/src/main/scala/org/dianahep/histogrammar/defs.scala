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

import scala.collection.immutable.ListMap
import scala.language.existentials
import scala.language.implicitConversions

import org.dianahep.histogrammar.json._

package histogrammar {
  /** Exception type for improperly configured containers. */
  class ContainerException(message: String, cause: Exception = null) extends Exception(message, cause)

  //////////////////////////////////////////////////////////////// general definition of an container, its factory, and mix-in

  /** Interface for a container factory, always named as imperative verbs, such as "Count" and "Bin".
    * 
    * Each factory has:
    * 
    *    - a custom `apply` method to create an active container than can aggregate data (and is therefore mutable). This active container is named by the gerund form of the verb, such as "Counting" and "Binning".
    *    - a custom `ed` method to create a fixed container that cannot aggregate data (immutable), only merge with the `+` operator. This fixed container is named by the past tense form of the verb, such as "Counted" and "Binned".
    *    - a uniform `fromJsonFragment` method that can reconstruct a fixed (immutable) container from its JSON representation. This is used by the `Factory` object's `fromJson` entry point. (Click on the "t" in a circle in the upper-left to see the `Factory` object's documentation, rather than the `Factory` trait.
    *    - `unapply` methods to unpack active and fixed containers in Scala pattern matching.
    * 
    */
  trait Factory {
    /** Name of the concrete `Factory` as a string; used to label the container type in JSON. */
    def name: String
    /** Help text that can be queried interactively: a one-liner that can be included in a menu. */
    def help: String
    /** Help text that can be queried interactively: more detail than `help`. ('''FIXME:''' currently only contains the `apply` signature.) */
    def detailedHelp: String
    /** Reconstructs a container of known type from JSON. General users should call the `Factory` object's `fromJson`, which uses header data to identify the container type. (This is called by `fromJson`.) */
    def fromJsonFragment(json: Json, nameFromParent: Option[String]): Container[_] with NoAggregation
  }

  /** Entry point for constructing containers from JSON and centralized registry of container types.
    * 
    * Containers filled in Python or on remote sites are serialized as JSON that the `Factory` object can reconstruct. Reconstructed containers are fixed (immutable, cannot aggregate), but can be merged with the `+` operator. (Click on the "o" in a circle in the upper-left to see the `Factory` trait's documentation, which explains the difference.)
    * 
    * To do this, the `Factory` object must dispatch JSON to the appropriate container for interpretation. It therefore manages a global registry of container types (concrete instances of the `Factory` trait). General users are not expected to add to this registry, but they could if they want to.
    */
  object Factory {
    private var known = ListMap[String, Factory]()

    /** Get a list of registered containers as a `Map` from factory name to `Factory` object. */
    def registered = known

    /** Add a new `Factory` to the registry, introducing a new container type on the fly. General users usually wouldn't do this, but they could. This method is used internally to define the standard container types. */
    def register(factory: Factory) {
      known = known.updated(factory.name, factory)
    }

    register(Count)
    register(Sum)
    register(Average)
    register(Deviate)
    register(AbsoluteErr)
    register(Minimize)
    register(Maximize)
    register(Quantile)

    register(Bin)
    register(SparselyBin)
    register(CentrallyBin)
    register(AdaptivelyBin)
    register(Categorize)

    register(Fraction)
    register(Stack)
    register(Partition)

    register(Select)
    register(Limit)
    register(Label)
    register(UntypedLabel)
    register(Index)
    register(Branch)

    register(Bag)
    register(Sample)

    /** Get a registered container by its name. */
    def apply(name: String) = known.get(name) match {
      case Some(x) => x
      case None => throw new ContainerException(s"unrecognized container (is it a custom container that hasn't been registered?): $name")
    }

    /** User's entry point for reconstructing a container from JSON text.
      * 
      * The container's type is not known at compile-time, so it must be cast (with the container's `as` method) or pattern-matched (with the corresponding `Factory`).
      */
    def fromJson(str: String): Container[_] with NoAggregation = Json.parse(str) match {
      case Some(json) => fromJson(json)
      case None => throw new InvalidJsonException(str)
    }

    /** User's entry point for reconstructing a container from a JSON object.
      * 
      * The container's type is not known at compile-time, so it must be cast (with the container's `as` method) or pattern-matched (with the corresponding `Factory`).
      */
    def fromJson(json: Json): Container[_] with NoAggregation = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("type", "data")) =>
        val get = pairs.toMap

        val name = get("type") match {
          case JsonString(x) => x
          case x => throw new JsonFormatException(x, "Factory.type")
        }

        Factory(name).fromJsonFragment(get("data"), None)

      case _ => throw new JsonFormatException(json, "Factory")
    }

    /** User's entry point for reading a container as JSON from a UTF-8 encoded file.
      * 
      * The container's type is not known at compile-time, so it must be cast (with the container's `as` method) or pattern-matched (with the corresponding `Factory`).
      */
    def fromJsonFile(fileName: String): Container[_] with NoAggregation = fromJsonFile(new java.io.File(fileName))

    /** User's entry point for reading a container as JSON from a UTF-8 encoded file.
      * 
      * The container's type is not known at compile-time, so it must be cast (with the container's `as` method) or pattern-matched (with the corresponding `Factory`).
      */
    def fromJsonFile(file: java.io.File): Container[_] with NoAggregation = fromJsonStream(new java.io.FileInputStream(file))

    /** User's entry point for reading a container as JSON from a UTF-8 encoded file.
      * 
      * Note: fully consumes the `inputStream`.
      * 
      * The container's type is not known at compile-time, so it must be cast (with the container's `as` method) or pattern-matched (with the corresponding `Factory`).
      */
    def fromJsonStream(inputStream: java.io.InputStream): Container[_] with NoAggregation =
      fromJson(new java.util.Scanner(inputStream).useDelimiter("\\A").next)
  }

  /** Interface for classes that contain aggregated data, such as "Counted" or "Binned" (immutable) or "Counting" or "Binning" (mutable).
    * 
    * There are two "tenses" of containers: present tense ("Counting", "Binning", etc.) that additionally mix-in [[org.dianahep.histogrammar.Aggregation]] and have a `fill` method for accumulating data, and past tense ("Counted", "Binned", etc.) that can only be merged with the `+` operator.
    * 
    * Containers are monoids: they have a neutral element (`zero`) and an associative operator (`+`). Thus, partial sums aggregated in parallel can be combined arbitrarily.
    * 
    * The `Container` is parameterized by itself (an example of the [[https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern curiously recurring template pattern]]) to pass type information at compile-time.
    */
  trait Container[CONTAINER <: Container[CONTAINER]] extends Serializable {
    /** Intended for the general user to copy a complex container's type into the `as` method of a container whose type is not known at compile-time.
      * 
      * Typical use: `filledHistogram.as[initialHistogram.Type]`
      */
    type Type
    /** The type of the immutable version of this container. */
    type EdType <: Container[EdType] with NoAggregation
    /** Reference to the container's factory for runtime reflection. */
    def factory: Factory

    /** Every `Container` accumulates a sum of weights of observed data.
      * 
      * The [[org.dianahep.histogrammar.Counting]]/[[org.dianahep.histogrammar.Counted]] container ''only'' accumulates a sum of weights.
      * 
      * Its data type is `Double` because in principal, it can be any non-negative real number.
      */
    def entries: Double
    /** Create an empty container with the same parameters as this one.
      * 
      * If this container is mutable (with [[org.dianahep.histogrammar.Aggregation]]), the new one will be, too.
      * 
      * The original is unaffected.
      */
    def zero: CONTAINER
    /** Add two containers of the same type.
      * 
      * If these containers are mutable (with [[org.dianahep.histogrammar.Aggregation]]), the new one will be, too.
      * 
      * The originals are unaffected.
      */
    def +(that: CONTAINER): CONTAINER
    /** Copy this container, making a clone with no reference to the original.
      * 
      * If these containers are mutable (with [[org.dianahep.histogrammar.Aggregation]]), the new one will be, too.
      */
    def copy = this + zero

    /** Convert this container to JSON (dropping its `fill` method, making it immutable).
      * 
      * Note that the [[org.dianahep.histogrammar.json.Json]] object has a `stringify` method to serialize.
      */
    def toJson: Json = JsonObject("type" -> JsonString(factory.name), "data" -> toJsonFragment(false))
    /** Used internally to convert the container to JSON without its `"type"` header. */
    def toJsonFragment(suppressName: Boolean): Json
    /** Convert any Container into a NoAggregation Container. */
    def ed: EdType = Factory.fromJson(toJson).asInstanceOf[EdType]
    /** Cast the container to a given type. Especially useful for containers reconstructed from JSON or stored in [[org.dianahep.histogrammar.UntypedLabeling]]/[[org.dianahep.histogrammar.UntypedLabeled]]. */
    def as[OTHER <: Container[OTHER]] = this.asInstanceOf[OTHER]

    /** List of sub-aggregators, to make it possible to walk the tree. */
    def children: Seq[Container[_]]
  }

  /** Mix-in to provide a quantity name for immutable Containers (analogous to [[org.dianahep.histogrammar.AnyQuantity]] for mutable Containers). */
  trait QuantityName {
    def quantityName: Option[String]
  }

  /** Mix-in to declare that the [[org.dianahep.histogrammar.Container]] is immutable (opposite of [[org.dianahep.histogrammar.Aggregation]]). */
  trait NoAggregation

  /** Mix-in to add mutability to a [[org.dianahep.histogrammar.Container]].
    * 
    * Containers without `Aggregation` can only be merged with the `+` operator, but containers with `Aggregation` can additionally be accumulated with `fill`.
    * 
    * Containers without `Aggregation` are named as past-tense verbs, such as "Counted" and "Binned", which containers with `Aggregation` are named with the gerund form, such as "Counting" and "Binning".
    * 
    * `Aggregation` is parameterized by the fill data type `Datum`, which is an abstract type member rather than a type parameter (square brackets) for better type inference.
    * 
    * This data type is implemented as contravariant: a container that expects to be filled with a given data type can accept that data type's subclass.
    */
  trait Aggregation {
    /** Type of data expected by `fill`. */
    type Datum

    /** The `entries` member of mutable containers is a `var`, rather than `val`. */
    def entries_=(x: Double)
    /** Entry point for the general user to pass data into the container for aggregation.
      * 
      * Usually all containers in a collection of histograms take the same input data by passing it recursively through the tree. Quantities to plot are specified by the individual container's lambda functions.
      * 
      * The container is changed in-place.
      */
    def fill[SUB <: Datum](datum: SUB, weight: Double = 1.0)
  }

  /** Sub-trait of [[org.dianahep.histogrammar.Aggregation]] for all containers except [[org.dianahep.histogrammar.Counting]].
    * 
    * `AggregationOnData` containers actually depend on their `Datum` type; `Counting` is the only one that ignores it.
    */
  trait AggregationOnData extends Aggregation

  /** Trait for aggregations that use a fill rule of any type. */
  trait AnyQuantity[DATUM, RANGE] {
    def quantity: UserFcn[DATUM, RANGE]
  }

  /** Trait for aggregations that use a numerical (double-valued) fill rule. */
  trait NumericalQuantity[DATUM] extends AnyQuantity[DATUM, Double]

  /** Trait for aggregations that use a categorical (string-valued) fill rule. */
  trait CategoricalQuantity[DATUM] extends AnyQuantity[DATUM, String]

  /** Increment function for Apache Spark's `aggregate` method.
    * 
    * Typical use: `filledHistogram = datasetRDD.aggregate(initialHistogram)(increment[initialHistogram.Type], combine[initialHistogram.Type])` where `datasetRDD` is a collection on `initialHistogram`'s `Datum` type.
    */
  class Increment[DATUM, CONTAINER <: Container[CONTAINER] with AggregationOnData {type Datum >: DATUM}] extends Function2[CONTAINER, DATUM, CONTAINER] with Serializable {
    def apply(h: CONTAINER, x: DATUM): CONTAINER = {h.fill(x); h}
  }

  /** Combine function for Apache Spark's `aggregate` method.
    * 
    * Typical use: `filledHistogram = datasetRDD.aggregate(initialHistogram)(increment[initialHistogram.Type], combine[initialHistogram.Type])` where `datasetRDD` is a collection on `initialHistogram`'s `Datum` type.
    */
  class Combine[CONTAINER <: Container[CONTAINER]] extends Function2[CONTAINER, CONTAINER, CONTAINER] with Serializable {
    def apply(h1: CONTAINER, h2: CONTAINER): CONTAINER = h1 + h2
  }
}

/** Main library for Histogrammar.
  * 
  * Defines all types for the general user, including implicits to construct Histogrammar-specific types from Scala basic types.
  * 
  * A general user is expected to `import org.dianahep.histogrammar._` to bring all of these implicits into scope.
  */
package object histogrammar {
  /** Help function for interactive use. Used to discover container types. */
  def help = Factory.registered map {case (name, factory) => f"${name}%-15s ${factory.help}"} mkString("\n")

  //////////////////////////////////////////////////////////////// define implicits

  /** Base trait for user functions. */
  trait UserFcn[-DOMAIN, +RANGE] extends Serializable {
    /** Optional name for the function; added to JSON for bookkeeping if present. */
    def name: Option[String]
    /** Tracks whether this function has a cache to ensure that a function doesn't get double-cached. */
    def hasCache: Boolean
    /** Call the function. */
    def apply[SUB <: DOMAIN](x: SUB): RANGE

    /** Tracks whether this function has a name to raise an error if it gets named again. */
    def hasName = !name.isEmpty

    /** Create a named version of this function.
      * 
      * Note that the `{x: Datum => f(x)} named "something"` syntax is more human-readable.
      * 
      * Note that this function commutes with `cached` (they can be applied in either order).
      */
    def named(n: String) =
      if (hasName)
        throw new IllegalArgumentException(s"two names applied to the same function: ${name.get} and $n")
      else {
        val f = this
        new UserFcn[DOMAIN, RANGE] {
          def name = Some(n)
          def hasCache = f.hasCache
          def apply[SUB <: DOMAIN](x: SUB): RANGE = f(x)
        }
      }

    /** Create a cached version of this function.
      * 
      * Note that the `{x: Datum => f(x)} cached` syntax is more human-readable.
      * 
      * Note that this function commutes with `named` (they can be applied in either order).
      * 
      * '''Example:'''
      * 
      * {{{
      * val f = cache {x: Double => complexFunction(x)}
      * f(3.14)   // computes the function
      * f(3.14)   // re-uses the old value
      * f(4.56)   // computes the function again at a new point
      * }}}
      */
    def cached =
      if (hasCache)
        this
      else {
        val f = this
        new UserFcn[DOMAIN, RANGE] {
          private var last: Option[(DOMAIN, RANGE)] = None
          def name = f.name
          def hasCache = true
          def apply[SUB <: DOMAIN](x: SUB): RANGE = (x, last) match {
            case (xref: AnyRef, Some((oldx: AnyRef, oldy))) if (xref eq oldx) => oldy
            case (_,            Some((oldx, oldy)))         if (x == oldx)    => oldy
            case _ =>
              val y = f(x)
              last = Some(x -> y)
              y
          }
        }
      }
  }

  implicit class NumericalFcnFromBoolean[-DATUM](f: DATUM => Boolean) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = if (f(x)) 1.0 else 0.0
  }
  implicit class NumericalFcnFromByte[-DATUM](f: DATUM => Byte) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x).toDouble
  }
  implicit class NumericalFcnFromShort[-DATUM](f: DATUM => Short) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x).toDouble
  }
  implicit class NumericalFcnFromInt[-DATUM](f: DATUM => Int) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x).toDouble
  }
  implicit class NumericalFcnFromLong[-DATUM](f: DATUM => Long) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x).toDouble
  }
  implicit class NumericalFcnFromFloat[-DATUM](f: DATUM => Float) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x).toDouble
  }
  implicit class NumericalFcnFromDouble[-DATUM](f: DATUM => Double) extends UserFcn[DATUM, Double] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Double = f(x)
  }

  /** Wraps a user's function for extracting strings (categories) from the input data type. */
  implicit class CategoricalFcn[-DATUM](f: DATUM => String) extends UserFcn[DATUM, String] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): String = f(x)
  }

  /** Wraps a user's function for extracting multidimensional numeric data from the input data type. */
  implicit class MultivariateFcn[-DATUM](f: DATUM => Iterable[Double]) extends UserFcn[DATUM, Vector[Double]] {
    def name = None
    def hasCache = false
    def apply[SUB <: DATUM](x: SUB): Vector[Double] = f(x).toVector
  }

  /** Default weighting function that always returns 1.0. */
  def unweighted[DATUM] = new UserFcn[DATUM, Double] {
    def name = Some("unweighted")
    def hasCache = true
    def apply[SUB <: DATUM](x: SUB) = 1.0
  }

  /** Introduces a `===` operator for all `Double` tolerance comparisons.
    * 
    * Custom equality rules:
    * 
    *   - NaN == NaN (NaNs are used by some primitives to indicate missing data).
    *   - if `org.dianahep.histogrammar.util.relativeTolerance` is greater than zero, numbers may differ by this small ratio.
    *   - if `org.dianahep.histogrammar.util.absoluteTolerance` is greater than zero, numbers may differ by this small difference.
    * 
    * Python's math.isclose algorithm is applied for non-NaNs:
    * 
    *     `abs(x - y) <= max(relativeTolerance * max(abs(x), abs(y)), absoluteTolerance)`
    */
  implicit class Numeq(val x: Double) extends AnyVal {
    def ===(that: Double) =
      if (this.x.isNaN  &&  that.isNaN)
        true
      else if (util.relativeTolerance > 0.0  &&  util.absoluteTolerance > 0.0)
        Math.abs(this.x - that) <= Math.max(util.relativeTolerance * Math.max(Math.abs(this.x), Math.abs(that)), util.absoluteTolerance)
      else if (util.relativeTolerance > 0.0)
        Math.abs(this.x - that) <= util.relativeTolerance * Math.max(Math.abs(this.x), Math.abs(that))
      else if (util.absoluteTolerance > 0.0)
        Math.abs(this.x - that) <= util.absoluteTolerance
      else
        this.x == that
  }

  //////////////////////////////////////////////////////////////// indexes for nested collections

  sealed trait CollectionIndex

  implicit class IntegerIndex(val int: Int) extends CollectionIndex {
    override def toString() = int.toString
  }
  object IntegerIndex {
    def unapply(x: IntegerIndex) = Some(x.int)
  }

  implicit class StringIndex(val string: String) extends CollectionIndex {
    override def toString() = "\"" + scala.util.parsing.json.JSONFormat.quoteString(string) + "\""
  }
  object StringIndex {
    def unapply(x: StringIndex) = Some(x.string)
  }

  implicit class SymbolIndex(val symbol: Symbol) extends CollectionIndex {
    override def toString() = symbol.toString
  }
  object SymbolIndex {
    def unapply(x: SymbolIndex) = Some(x.symbol)
  }

  trait Collection {
    def apply(indexes: CollectionIndex*): Container[_]

    def walk[X](op: Seq[CollectionIndex] => X): Seq[X] = walk(op, Seq[CollectionIndex]())

    private def walk[X](op: Seq[CollectionIndex] => X, base: Seq[CollectionIndex]): Seq[X] = this match {
      case labeled: Labeled[_] => labeled.pairs.flatMap {
        case (k, vs: Collection) => vs.walk(op, base :+ StringIndex(k))
        case (k, v) => Seq(op(base :+ StringIndex(k)))
      }

      case labeling: Labeling[_] => labeling.pairs.flatMap {
        case (k, vs: Collection) => vs.walk(op, base :+ StringIndex(k))
        case (k, v) => Seq(op(base :+ StringIndex(k)))
      }

      case untypedLabeled: UntypedLabeled => untypedLabeled.pairs.flatMap {
        case (k, vs: Collection) => vs.walk(op, base :+ StringIndex(k))
        case (k, v) => Seq(op(base :+ StringIndex(k)))
      }

      case untypedLabeling: UntypedLabeling[_] => untypedLabeling.pairs.flatMap {
        case (k, vs: Collection) => vs.walk(op, base :+ StringIndex(k))
        case (k, v) => Seq(op(base :+ StringIndex(k)))
      }

      case indexed: Indexed[_] => indexed.values.zipWithIndex.flatMap {
        case (vs: Collection, i) => vs.walk(op, base :+ IntegerIndex(i))
        case (v, i) => Seq(op(base :+ IntegerIndex(i)))
      }

      case indexing: Indexing[_] => indexing.values.zipWithIndex.flatMap {
        case (vs: Collection, i) => vs.walk(op, base :+ IntegerIndex(i))
        case (v, i) => Seq(op(base :+ IntegerIndex(i)))
      }

      case branched: Branched[_, _] => branched.values.zipWithIndex.flatMap {
        case (vs: Collection, 0) => vs.walk(op, base :+ SymbolIndex('i0))
        case (vs: Collection, 1) => vs.walk(op, base :+ SymbolIndex('i1))
        case (vs: Collection, 2) => vs.walk(op, base :+ SymbolIndex('i2))
        case (vs: Collection, 3) => vs.walk(op, base :+ SymbolIndex('i3))
        case (vs: Collection, 4) => vs.walk(op, base :+ SymbolIndex('i4))
        case (vs: Collection, 5) => vs.walk(op, base :+ SymbolIndex('i5))
        case (vs: Collection, 6) => vs.walk(op, base :+ SymbolIndex('i6))
        case (vs: Collection, 7) => vs.walk(op, base :+ SymbolIndex('i7))
        case (vs: Collection, 8) => vs.walk(op, base :+ SymbolIndex('i8))
        case (vs: Collection, 9) => vs.walk(op, base :+ SymbolIndex('i9))
        case (vs: Collection, i) => vs.walk(op, base :+ IntegerIndex(i))
        case (v, 0) => Seq(op(base :+ SymbolIndex('i0)))
        case (v, 1) => Seq(op(base :+ SymbolIndex('i1)))
        case (v, 2) => Seq(op(base :+ SymbolIndex('i2)))
        case (v, 3) => Seq(op(base :+ SymbolIndex('i3)))
        case (v, 4) => Seq(op(base :+ SymbolIndex('i4)))
        case (v, 5) => Seq(op(base :+ SymbolIndex('i5)))
        case (v, 6) => Seq(op(base :+ SymbolIndex('i6)))
        case (v, 7) => Seq(op(base :+ SymbolIndex('i7)))
        case (v, 8) => Seq(op(base :+ SymbolIndex('i8)))
        case (v, 9) => Seq(op(base :+ SymbolIndex('i9)))
        case (v, i) => Seq(op(base :+ IntegerIndex(i)))
      }

      case branching: Branching[_, _] => branching.values.zipWithIndex.flatMap {
        case (vs: Collection, 0) => vs.walk(op, base :+ SymbolIndex('i0))
        case (vs: Collection, 1) => vs.walk(op, base :+ SymbolIndex('i1))
        case (vs: Collection, 2) => vs.walk(op, base :+ SymbolIndex('i2))
        case (vs: Collection, 3) => vs.walk(op, base :+ SymbolIndex('i3))
        case (vs: Collection, 4) => vs.walk(op, base :+ SymbolIndex('i4))
        case (vs: Collection, 5) => vs.walk(op, base :+ SymbolIndex('i5))
        case (vs: Collection, 6) => vs.walk(op, base :+ SymbolIndex('i6))
        case (vs: Collection, 7) => vs.walk(op, base :+ SymbolIndex('i7))
        case (vs: Collection, 8) => vs.walk(op, base :+ SymbolIndex('i8))
        case (vs: Collection, 9) => vs.walk(op, base :+ SymbolIndex('i9))
        case (vs: Collection, i) => vs.walk(op, base :+ IntegerIndex(i))
        case (v, 0) => Seq(op(base :+ SymbolIndex('i0)))
        case (v, 1) => Seq(op(base :+ SymbolIndex('i1)))
        case (v, 2) => Seq(op(base :+ SymbolIndex('i2)))
        case (v, 3) => Seq(op(base :+ SymbolIndex('i3)))
        case (v, 4) => Seq(op(base :+ SymbolIndex('i4)))
        case (v, 5) => Seq(op(base :+ SymbolIndex('i5)))
        case (v, 6) => Seq(op(base :+ SymbolIndex('i6)))
        case (v, 7) => Seq(op(base :+ SymbolIndex('i7)))
        case (v, 8) => Seq(op(base :+ SymbolIndex('i8)))
        case (v, 9) => Seq(op(base :+ SymbolIndex('i9)))
        case (v, i) => Seq(op(base :+ IntegerIndex(i)))
      }
    }

    def indexes = walk((x: Seq[CollectionIndex]) => x).toSeq
  }

  //////////////////////////////////////////////////////////////// typesafe extractors for Branched/Branching

  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched0[C0 <: Container[C0] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, TAIL]) {
    def i0 = x.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched1[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, TAIL]]) {
    def i1 = x.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched2[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, TAIL]]]) {
    def i2 = x.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched3[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, TAIL]]]]) {
    def i3 = x.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched4[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, TAIL]]]]]) {
    def i4 = x.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched5[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, C5 <: Container[C5] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, Branched[C5, TAIL]]]]]]) {
    def i5 = x.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched6[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, C5 <: Container[C5] with NoAggregation, C6 <: Container[C6] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, Branched[C5, Branched[C6, TAIL]]]]]]]) {
    def i6 = x.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched7[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, C5 <: Container[C5] with NoAggregation, C6 <: Container[C6] with NoAggregation, C7 <: Container[C7] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, Branched[C5, Branched[C6, Branched[C7, TAIL]]]]]]]]) {
    def i7 = x.tail.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched8[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, C5 <: Container[C5] with NoAggregation, C6 <: Container[C6] with NoAggregation, C7 <: Container[C7] with NoAggregation, C8 <: Container[C8] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, Branched[C5, Branched[C6, Branched[C7, Branched[C8, TAIL]]]]]]]]]) {
    def i8 = x.tail.tail.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branched` containers. */
  implicit class Branched9[C0 <: Container[C0] with NoAggregation, C1 <: Container[C1] with NoAggregation, C2 <: Container[C2] with NoAggregation, C3 <: Container[C3] with NoAggregation, C4 <: Container[C4] with NoAggregation, C5 <: Container[C5] with NoAggregation, C6 <: Container[C6] with NoAggregation, C7 <: Container[C7] with NoAggregation, C8 <: Container[C8] with NoAggregation, C9 <: Container[C9] with NoAggregation, TAIL <: BranchedList](x: Branched[C0, Branched[C1, Branched[C2, Branched[C3, Branched[C4, Branched[C5, Branched[C6, Branched[C7, Branched[C8, Branched[C9, TAIL]]]]]]]]]]) {
    def i9 = x.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  }

  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching0[C0 <: Container[C0] with Aggregation, TAIL <: BranchingList](x: Branching[C0, TAIL]) {
    def i0 = x.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching1[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, TAIL]]) {
    def i1 = x.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching2[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, TAIL]]]) {
    def i2 = x.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching3[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, TAIL]]]]) {
    def i3 = x.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching4[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, TAIL]]]]]) {
    def i4 = x.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching5[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, C5 <: Container[C5] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, Branching[C5, TAIL]]]]]]) {
    def i5 = x.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching6[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, C5 <: Container[C5] with Aggregation, C6 <: Container[C6] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, Branching[C5, Branching[C6, TAIL]]]]]]]) {
    def i6 = x.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching7[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, C5 <: Container[C5] with Aggregation, C6 <: Container[C6] with Aggregation, C7 <: Container[C7] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, Branching[C5, Branching[C6, Branching[C7, TAIL]]]]]]]]) {
    def i7 = x.tail.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching8[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, C5 <: Container[C5] with Aggregation, C6 <: Container[C6] with Aggregation, C7 <: Container[C7] with Aggregation, C8 <: Container[C8] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, Branching[C5, Branching[C6, Branching[C7, Branching[C8, TAIL]]]]]]]]]) {
    def i8 = x.tail.tail.tail.tail.tail.tail.tail.tail.head
  }
  /** Add `i0`, `i1`, etc. methods to `Branching` containers. */
  implicit class Branching9[C0 <: Container[C0] with Aggregation, C1 <: Container[C1] with Aggregation, C2 <: Container[C2] with Aggregation, C3 <: Container[C3] with Aggregation, C4 <: Container[C4] with Aggregation, C5 <: Container[C5] with Aggregation, C6 <: Container[C6] with Aggregation, C7 <: Container[C7] with Aggregation, C8 <: Container[C8] with Aggregation, C9 <: Container[C9] with Aggregation, TAIL <: BranchingList](x: Branching[C0, Branching[C1, Branching[C2, Branching[C3, Branching[C4, Branching[C5, Branching[C6, Branching[C7, Branching[C8, Branching[C9, TAIL]]]]]]]]]]) {
    def i9 = x.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  }

  //////////////////////////////////////////////////////////////// type alias for Histogram

  /** Type alias for conventional histograms (filled). */
  type Histogrammed = Selected[Binned[Counted, Counted, Counted, Counted]]
  /** Type alias for conventional histograms (filling). */
  type Histogramming[DATUM] = Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]]
  /** Convenience function for creating a conventional histogram. */
  def Histogram[DATUM]
    (num: Int,
    low: Double,
    high: Double,
    quantity: UserFcn[DATUM, Double],
    selection: UserFcn[DATUM, Double] = unweighted[DATUM]) =
    Select(selection, Bin(num, low, high, quantity))

  //////////////////////////////////////////////////////////////// type alias for SparselyHistogram

  /** Type alias for sparsely binned histograms (filled). */
  type SparselyHistogrammed = Selected[SparselyBinned[Counted, Counted]]
  /** Type alias for sparsely binned histograms (filling). */
  type SparselyHistogramming[DATUM] = Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]]
  /** Convenience function for creating a sparsely binned histogram. */
  def SparselyHistogram[DATUM]
    (binWidth: Double,
    quantity: UserFcn[DATUM, Double],
    selection: UserFcn[DATUM, Double] = unweighted[DATUM],
    origin: Double = 0.0) =
    Select(selection, SparselyBin(binWidth, quantity, origin = origin))

  //////////////////////////////////////////////////////////////// methods for Histogram and SparselyHistogram

  implicit def binnedToHistogramMethods(hist: Binned[Counted, Counted, Counted, Counted]): HistogramMethods =
    new HistogramMethods(new Selected(hist.entries, unweighted.name, hist))

  implicit def binningToHistogramMethods[DATUM](hist: Binning[DATUM, Counting, Counting, Counting, Counting]): HistogramMethods =
    binnedToHistogramMethods(hist.ed)

  implicit def selectedBinnedToHistogramMethods(hist: Selected[Binned[Counted, Counted, Counted, Counted]]): HistogramMethods =
    new HistogramMethods(hist)

  implicit def selectingBinningToHistogramMethods[DATUM](hist: Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]]): HistogramMethods =
    selectedBinnedToHistogramMethods(hist.ed)

  implicit def sparselyBinnedToHistogramMethods(hist: SparselyBinned[Counted, Counted]): HistogramMethods =
    selectedSparselyBinnedToHistogramMethods(new Selected(hist.entries, unweighted.name, hist))

  implicit def sparselyBinningToHistogramMethods[DATUM](hist: SparselyBinning[DATUM, Counting, Counting]): HistogramMethods =
    sparselyBinnedToHistogramMethods(hist.ed)

  implicit def selectedSparselyBinnedToHistogramMethods(hist: Selected[SparselyBinned[Counted, Counted]]): HistogramMethods =
    if (hist.cut.numFilled > 0)
      new HistogramMethods(
        new Selected(hist.entries, hist.quantityName, new Binned(hist.cut.low.get, hist.cut.high.get, hist.cut.entries, hist.cut.quantityName, hist.cut.minBin.get to hist.cut.maxBin.get map {i => new Counted(hist.cut.at(i).flatMap(x => Some(x.entries)).getOrElse(0L))}, new Counted(0.0), new Counted(0.0), hist.cut.nanflow))
      )
    else
      new HistogramMethods(
        new Selected(hist.entries, hist.quantityName, new Binned(hist.cut.origin, hist.cut.origin + 1.0, hist.cut.entries, hist.cut.quantityName, Seq(new Counted(0.0)), new Counted(0.0), new Counted(0.0), hist.cut.nanflow))
      )

  implicit def selectingSparselyBinningToHistogramMethods[DATUM](hist: Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]]): HistogramMethods =
    selectedSparselyBinnedToHistogramMethods(hist.ed)

  /** Methods that are implicitly added to container combinations that look like histograms. */
  class HistogramMethods(val selected: Selected[Binned[Counted, Counted, Counted, Counted]]) {
    /** Access the [[org.dianahep.histogrammar.Binned]] object, rather than the [[org.dianahep.histogrammar.Selected]] wrapper. */
    def binned = selected.cut

    /** Bin values as numbers, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalValues: Seq[Double] = binned.values.map(_.entries)
    /** Overflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalOverflow: Double = binned.overflow.entries
    /** Underflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalUnderflow: Double = binned.underflow.entries
    /** Nanflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalNanflow: Double = binned.nanflow.entries
  }

  //////////////////////////////////////////////////////////////// type alias for Profile

  /** Type alias for a physicist's "profile plot" (filled). */
  type Profiled = Selected[Binned[Deviated, Counted, Counted, Counted]]
  /** Type alias for a physicist's "profile plot" (filling). */
  type Profiling[DATUM] = Selecting[DATUM, Binning[DATUM, Deviating[DATUM], Counting, Counting, Counting]]
  /** Convenience function for creating a physicist's "profile plot." */
  def Profile[DATUM]
    (num: Int,
    low: Double,
    high: Double,
    binnedQuantity: UserFcn[DATUM, Double],
    averagedQuantity: UserFcn[DATUM, Double],
    selection: UserFcn[DATUM, Double] = unweighted[DATUM]) =
    Select(selection, Bin(num, low, high, binnedQuantity, Deviate(averagedQuantity)))

  //////////////////////////////////////////////////////////////// type alias for SparselyProfile

  /** Type alias for a physicist's sparsely binned "profile plot" (filled). */
  type SparselyProfiled = Selected[SparselyBinned[Deviated, Counted]]
  /** Type alias for a physicist's sparsely binned "profile plot" (filling). */
  type SparselyProfiling[DATUM] = Selecting[DATUM, SparselyBinning[DATUM, Deviating[DATUM], Counting]]
  /** Convenience function for creating a physicist's sparsely binned "profile plot." */
  def SparselyProfile[DATUM]
    (binWidth: Double,
    binnedQuantity: UserFcn[DATUM, Double],
    averagedQuantity: UserFcn[DATUM, Double],
    selection: UserFcn[DATUM, Double] = unweighted[DATUM]) =
    Select(selection, SparselyBin(binWidth, binnedQuantity, Deviate(averagedQuantity)))

  //////////////////////////////////////////////////////////////// methods for Profile and SparselyProfile

  implicit def binnedToProfileMethods(hist: Binned[Deviated, Counted, Counted, Counted]): ProfileMethods =
    new ProfileMethods(new Selected(hist.entries, unweighted.name, hist))

  implicit def binningToProfileMethods[DATUM](hist: Binning[DATUM, Deviating[DATUM], Counting, Counting, Counting]): ProfileMethods =
    binnedToProfileMethods(hist.ed)

  implicit def selectedBinnedToProfileMethods(hist: Selected[Binned[Deviated, Counted, Counted, Counted]]): ProfileMethods =
    new ProfileMethods(hist)

  implicit def selectingBinningToProfileMethods[DATUM](hist: Selecting[DATUM, Binning[DATUM, Deviating[DATUM], Counting, Counting, Counting]]): ProfileMethods =
    selectedBinnedToProfileMethods(hist.ed)

  implicit def sparselyBinnedToProfileMethods(hist: SparselyBinned[Deviated, Counted]): ProfileMethods =
    selectedSparselyBinnedToProfileMethods(new Selected(hist.entries, unweighted.name, hist))

  implicit def sparselyBinningToProfileMethods[DATUM](hist: SparselyBinning[DATUM, Deviating[DATUM], Counting]): ProfileMethods =
    sparselyBinnedToProfileMethods(hist.ed)

  implicit def selectedSparselyBinnedToProfileMethods(hist: Selected[SparselyBinned[Deviated, Counted]]): ProfileMethods =
    if (hist.cut.numFilled > 0)
      new ProfileMethods(
        new Selected(hist.entries, hist.quantityName, new Binned(hist.cut.low.get, hist.cut.high.get, hist.cut.entries, hist.cut.quantityName, hist.cut.minBin.get to hist.cut.maxBin.get map {i => hist.cut.at(i).getOrElse(new Deviated(0.0, None, 0.0, 0.0))}, new Counted(0.0), new Counted(0.0), hist.cut.nanflow))
      )
    else
      new ProfileMethods(
        new Selected(hist.entries, hist.quantityName, new Binned(hist.cut.origin, hist.cut.origin + 1.0, hist.cut.entries, hist.cut.quantityName, Seq(new Deviated(0.0, None, 0.0, 0.0)), new Counted(0.0), new Counted(0.0), hist.cut.nanflow))
      )

  implicit def selectingSparselyBinningToProfileMethods[DATUM](hist: Selecting[DATUM, SparselyBinning[DATUM, Deviating[DATUM], Counting]]): ProfileMethods =
    selectedSparselyBinnedToProfileMethods(hist.ed)

  /** Methods that are implicitly added to container combinations that look like a physicist's "profile plot." */
  class ProfileMethods(val selected: Selected[Binned[Deviated, Counted, Counted, Counted]]) {
    def binned = selected.cut

    /** Bin means as numbers, rather than [[org.dianahep.histogrammar.Deviated]]/[[org.dianahep.histogrammar.Deviating]]. */
    def meanValues: Seq[Double] = binned.values.map(_.mean)
    /** Bin variances as numbers, rather than [[org.dianahep.histogrammar.Deviated]]/[[org.dianahep.histogrammar.Deviating]]. */
    def varianceValues: Seq[Double] = binned.values.map(_.variance)

    /** Overflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalOverflow: Double = binned.overflow.entries
    /** Underflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalUnderflow: Double = binned.underflow.entries
    /** Nanflow as a number, rather than [[org.dianahep.histogrammar.Counted]]/[[org.dianahep.histogrammar.Counting]]. */
    def numericalNanflow: Double = binned.nanflow.entries
  }

  //////////////////////////////////////////////////////////////// methods for StackedHistogram, including cases for mixed tenses

  implicit def binnedToStackedHistogramMethods(hist: Stacked[Binned[Counted, Counted, Counted, Counted], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, new Selected(v.entries, unweighted.name, v))}), hist.nanflow))

  implicit def binningToStackedHistogramMethods[DATUM](hist: Stacking[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting], Counting]): StackedHistogramMethods =
    binnedToStackedHistogramMethods(hist.ed)

  implicit def selectedBinnedToStackedHistogramMethods(hist: Stacked[Selected[Binned[Counted, Counted, Counted, Counted]], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(hist)

  implicit def selectingBinningToStackedHistogramMethods[DATUM](hist: Stacking[DATUM, Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]], Counting]): StackedHistogramMethods =
    selectedBinnedToStackedHistogramMethods(hist.ed)

  implicit def sparselyBinnedToStackedHistogramMethods(hist: Stacked[SparselyBinned[Counted, Counted], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, sparselyBinnedToHistogramMethods(v).selected)}), hist.nanflow))

  implicit def sparselyBinningToStackingHistogramMethods[DATUM](hist: Stacking[DATUM, SparselyBinning[DATUM, Counting, Counting], Counting]): StackedHistogramMethods =
    sparselyBinnedToStackedHistogramMethods(hist.ed)

  implicit def selectedSparselyBinnedToStackedHistogramMethods(hist: Stacked[Selected[SparselyBinned[Counted, Counted]], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, selectedSparselyBinnedToHistogramMethods(v).selected)}), hist.nanflow))

  implicit def selectingSparselyBinningToStackedHistogramMethods[DATUM](hist: Stacking[DATUM, Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]], Counting]): StackedHistogramMethods =
    selectedSparselyBinnedToStackedHistogramMethods(hist.ed)

  implicit def binnedMixedToStackedHistogramMethods[DATUM](hist: Stacked[Binning[DATUM, Counting, Counting, Counting, Counting], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, new Selected(v.entries, unweighted.name, v.ed))}), hist.nanflow))

  implicit def selectedBinnedMixedToStackedHistogramMethods[DATUM](hist: Stacked[Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, v.ed)}), hist.nanflow))

  implicit def sparselyBinnedMixedToStackedHistogramMethods[DATUM](hist: Stacked[SparselyBinning[DATUM, Counting, Counting], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, sparselyBinnedToHistogramMethods(v.ed).selected)}), hist.nanflow))

  implicit def selectedSparselyBinnedMixedToStackedHistogramMethods[DATUM](hist: Stacked[Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]], Counted]): StackedHistogramMethods =
    new StackedHistogramMethods(new Stacked(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, selectedSparselyBinnedToHistogramMethods(v.ed).selected)}), hist.nanflow))

  /** Methods that are implicitly added to container combinations that look like stacked histograms. */
  class StackedHistogramMethods(val stacked: Stacked[Selected[Binned[Counted, Counted, Counted, Counted]], Counted]) {
    def low = stacked.cuts.head._2.cut.low
    def high = stacked.cuts.head._2.cut.high
    def num = stacked.cuts.head._2.cut.num

    stacked.values foreach {selected =>
      if (selected.cut.low != low)
        throw new ContainerException(s"Stack invalid because low values differ (${low} vs ${selected.cut.low})")
    }
    stacked.values foreach {selected =>
      if (selected.cut.high != high)
        throw new ContainerException(s"Stack invalid because high values differ (${high} vs ${selected.cut.high})")
    }
    stacked.values foreach {selected =>
      if (selected.cut.num != num)
        throw new ContainerException(s"Stack invalid because num values differ (${num} vs ${selected.cut.num})")
    }
  }

  //////////////////////////////////////////////////////////////// methods for PartitionedHistogram

  implicit def binnedToPartitionedHistogramMethods(hist: Partitioned[Binned[Counted, Counted, Counted, Counted], Counted]): PartitionedHistogramMethods =
    new PartitionedHistogramMethods(new Partitioned(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, new Selected(v.entries, unweighted.name, v))}), hist.nanflow))

  implicit def binningToPartitionedHistogramMethods[DATUM](hist: Partitioning[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting], Counting]): PartitionedHistogramMethods =
    binnedToPartitionedHistogramMethods(hist.ed)

  implicit def selectedBinnedToPartitionedHistogramMethods(hist: Partitioned[Selected[Binned[Counted, Counted, Counted, Counted]], Counted]): PartitionedHistogramMethods =
    new PartitionedHistogramMethods(hist)

  implicit def selectingBinningToPartitionedHistogramMethods[DATUM](hist: Partitioning[DATUM, Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]], Counting]): PartitionedHistogramMethods =
    selectedBinnedToPartitionedHistogramMethods(hist.ed)

  implicit def sparselyBinnedToPartitionedHistogramMethods(hist: Partitioned[SparselyBinned[Counted, Counted], Counted]): PartitionedHistogramMethods =
    new PartitionedHistogramMethods(new Partitioned(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, sparselyBinnedToHistogramMethods(v).selected)}), hist.nanflow))

  implicit def sparselyBinningToPartitioningHistogramMethods[DATUM](hist: Partitioning[DATUM, SparselyBinning[DATUM, Counting, Counting], Counting]): PartitionedHistogramMethods =
    sparselyBinnedToPartitionedHistogramMethods(hist.ed)

  implicit def selectedSparselyBinnedToPartitionedHistogramMethods(hist: Partitioned[Selected[SparselyBinned[Counted, Counted]], Counted]): PartitionedHistogramMethods =
    new PartitionedHistogramMethods(new Partitioned(hist.entries, hist.quantityName, hist.cuts.map({case (x, v) => (x, selectedSparselyBinnedToHistogramMethods(v).selected)}), hist.nanflow))

  implicit def selectingSparselyBinningToPartitionedHistogramMethods[DATUM](hist: Partitioning[DATUM, Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]], Counting]): PartitionedHistogramMethods =
    selectedSparselyBinnedToPartitionedHistogramMethods(hist.ed)

  /** Methods that are implicitly added to container combinations that look like partitioned histograms. */
  class PartitionedHistogramMethods(val partitioned: Partitioned[Selected[Binned[Counted, Counted, Counted, Counted]], Counted]) {
    def low = partitioned.cuts.head._2.cut.low
    def high = partitioned.cuts.head._2.cut.high
    def num = partitioned.cuts.head._2.cut.num

    partitioned.values foreach {selected =>
      if (selected.cut.low != low)
        throw new ContainerException(s"Partition invalid because low values differ (${low} vs ${selected.cut.low})")
    }
    partitioned.values foreach {selected =>
      if (selected.cut.high != high)
        throw new ContainerException(s"Partition invalid because high values differ (${high} vs ${selected.cut.high})")
    }
    partitioned.values foreach {selected =>
      if (selected.cut.num != num)
        throw new ContainerException(s"Partition invalid because num values differ (${num} vs ${selected.cut.num})")
    }
  }

  //////////////////////////////////////////////////////////////// methods for FractionedHistogram

  implicit def binnedToFractionedHistogramMethods(hist: Fractioned[Binned[Counted, Counted, Counted, Counted]]): FractionedHistogramMethods =
    new FractionedHistogramMethods(new Fractioned(hist.entries, hist.quantityName, new Selected(hist.numerator.entries, unweighted.name, hist.numerator), new Selected(hist.denominator.entries, unweighted.name, hist.denominator)))

  implicit def binningToFractionedHistogramMethods[DATUM](hist: Fractioning[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]]): FractionedHistogramMethods =
    binnedToFractionedHistogramMethods(hist.ed)

  implicit def selectedBinnedToFractionedHistogramMethods(hist: Fractioned[Selected[Binned[Counted, Counted, Counted, Counted]]]): FractionedHistogramMethods =
    new FractionedHistogramMethods(hist)

  implicit def selectingBinningToFractionedHistogramMethods[DATUM](hist: Fractioning[DATUM, Selecting[DATUM, Binning[DATUM, Counting, Counting, Counting, Counting]]]): FractionedHistogramMethods =
    selectedBinnedToFractionedHistogramMethods(hist.ed)

  implicit def sparselyBinnedToFractionedHistogramMethods(hist: Fractioned[SparselyBinned[Counted, Counted]]): FractionedHistogramMethods =
    new FractionedHistogramMethods(new Fractioned(hist.entries, hist.quantityName, sparselyBinnedToHistogramMethods(hist.numerator).selected, sparselyBinnedToHistogramMethods(hist.denominator).selected))

  implicit def sparselyBinningToFractioningHistogramMethods[DATUM](hist: Fractioning[DATUM, SparselyBinning[DATUM, Counting, Counting]]): FractionedHistogramMethods =
    sparselyBinnedToFractionedHistogramMethods(hist.ed)

  implicit def selectedSparselyBinnedToFractionedHistogramMethods(hist: Fractioned[Selected[SparselyBinned[Counted, Counted]]]): FractionedHistogramMethods =
    new FractionedHistogramMethods(new Fractioned(hist.entries, hist.quantityName, selectedSparselyBinnedToHistogramMethods(hist.numerator).selected, selectedSparselyBinnedToHistogramMethods(hist.denominator).selected))

  implicit def selectingSparselyBinningToFractionedHistogramMethods[DATUM](hist: Fractioning[DATUM, Selecting[DATUM, SparselyBinning[DATUM, Counting, Counting]]]): FractionedHistogramMethods =
    selectedSparselyBinnedToFractionedHistogramMethods(hist.ed)

  /** Methods that are implicitly added to container combinations that look like fractioned histograms. */
  class FractionedHistogramMethods(val fractioned: Fractioned[Selected[Binned[Counted, Counted, Counted, Counted]]]) {
    def numeratorBinned = fractioned.numerator.cut
    def denominatorBinned = fractioned.denominator.cut

    if (numeratorBinned.low != denominatorBinned.low)
      throw new ContainerException(s"Fraction invalid because low differs between numerator and denominator (${numeratorBinned.low} vs ${denominatorBinned.low})")
    if (numeratorBinned.high != denominatorBinned.high)
      throw new ContainerException(s"Fraction invalid because high differs between numerator and denominator (${numeratorBinned.high} vs ${denominatorBinned.high})")
    if (numeratorBinned.values.size != denominatorBinned.values.size)
      throw new ContainerException(s"Fraction invalid because number of values differs between numerator and denominator (${numeratorBinned.values.size} vs ${denominatorBinned.values.size})")

    def low = numeratorBinned.low
    def high = numeratorBinned.high
    def num = numeratorBinned.num

    /** Bin fractions as numbers. */
    def numericalValues: Seq[Double] = numeratorBinned.values zip denominatorBinned.values map {case (n, d) => n.entries / d.entries}
    /** Low-central-high confidence interval triplet for all bins, given a confidence interval function.
      * 
      * @param confidenceInterval confidence interval function, which takes (numerator entries, denominator entries, `z`) as arguments, where `z` is the "number of sigmas:" `z = 0` is the central value, `z = -1` is the 68% confidence level below the central value, and `z = 1` is the 68% confidence level above the central value.
      * @param absz absolute value of `z` to evaluate.
      * @return confidence interval evaluated at `(-absz, 0, absz)`.
      */
    def confidenceIntervalValues(confidenceInterval: (Double, Double, Double) => Double, absz: Double = 1.0): Seq[(Double, Double, Double)] = numeratorBinned.values zip denominatorBinned.values map {case (n, d) =>
      (confidenceInterval(n.entries, d.entries, -absz), confidenceInterval(n.entries, d.entries, 0.0), confidenceInterval(n.entries, d.entries, absz))
    }

    /** Overflow fraction as a number. */
    def numericalOverflow: Double = numeratorBinned.overflow.entries / denominatorBinned.overflow.entries
    /** Low-central-high confidence interval triplet for the overflow bin, given a confidence interval function.
      * 
      * @param confidenceInterval confidence interval function, which takes (numerator entries, denominator entries, `z`) as arguments, where `z` is the "number of sigmas:" `z = 0` is the central value, `z = -1` is the 68% confidence level below the central value, and `z = 1` is the 68% confidence level above the central value.
      * @param absz absolute value of `z` to evaluate.
      * @return confidence interval evaluated at `(-absz, 0, absz)`.
      */
    def confidenceIntervalOverflow(confidenceInterval: (Double, Double, Double) => Double, absz: Double = 1.0): (Double, Double, Double) = {
      val (n, d) = (numeratorBinned.overflow, denominatorBinned.overflow)
      (confidenceInterval(n.entries, d.entries, -absz), confidenceInterval(n.entries, d.entries, 0.0), confidenceInterval(n.entries, d.entries, absz))
    }

    /** Underflow fraction as a number. */
    def numericalUnderflow: Double = numeratorBinned.underflow.entries / denominatorBinned.underflow.entries
    /** Low-central-high confidence interval triplet for the overflow bin, given a confidence interval function.
      * 
      * @param confidenceInterval confidence interval function, which takes (numerator entries, denominator entries, `z`) as arguments, where `z` is the "number of sigmas:" `z = 0` is the central value, `z = -1` is the 68% confidence level below the central value, and `z = 1` is the 68% confidence level above the central value.
      * @param absz absolute value of `z` to evaluate.
      * @return confidence interval evaluated at `(-absz, 0, absz)`.
      */
    def confidenceIntervalUnderflow(confidenceInterval: (Double, Double, Double) => Double, absz: Double = 1.0): (Double, Double, Double) = {
      val (n, d) = (numeratorBinned.underflow, denominatorBinned.underflow)
      (confidenceInterval(n.entries, d.entries, -absz), confidenceInterval(n.entries, d.entries, 0.0), confidenceInterval(n.entries, d.entries, absz))
    }

    /** Nanflow fraction as a number. */
    def numericalNanflow: Double = numeratorBinned.nanflow.entries / denominatorBinned.nanflow.entries
    /** Low-central-high confidence interval triplet for the nanflow bin, given a confidence interval function.
      * 
      * @param confidenceInterval confidence interval function, which takes (numerator entries, denominator entries, `z`) as arguments, where `z` is the "number of sigmas:" `z = 0` is the central value, `z = -1` is the 68% confidence level below the central value, and `z = 1` is the 68% confidence level above the central value.
      * @param absz absolute value of `z` to evaluate.
      * @return confidence interval evaluated at `(-absz, 0, absz)`.
      */
    def confidenceIntervalNanflow(confidenceInterval: (Double, Double, Double) => Double, absz: Double = 1.0): (Double, Double, Double) = {
      val (n, d) = (numeratorBinned.nanflow, denominatorBinned.nanflow)
      (confidenceInterval(n.entries, d.entries, -absz), confidenceInterval(n.entries, d.entries, 0.0), confidenceInterval(n.entries, d.entries, absz))
    }
  }

  //////////////////////////////////////////////////////////////// methods for (nested) collections

  implicit def labeledToCollectionMethods(labeled: Labeled[_]) = new CollectionMethods(labeled)
  implicit def labelingToCollectionMethods(labeling: Labeling[_]) = new CollectionMethods(labeling)
  implicit def untypedLabeledToCollectionMethods(untypedLabeled: UntypedLabeled) = new CollectionMethods(untypedLabeled)
  implicit def untypedLabelingToCollectionMethods(untypedLabeling: UntypedLabeling[_]) = new CollectionMethods(untypedLabeling)
  implicit def indexedToCollectionMethods(indexed: Indexed[_]) = new CollectionMethods(indexed)
  implicit def indexingToCollectionMethods(indexing: Indexing[_]) = new CollectionMethods(indexing)
  implicit def branchedToCollectionMethods(branched: Branched[_, _]) = new CollectionMethods(branched)
  implicit def branchingToCollectionMethods(branching: Branching[_, _]) = new CollectionMethods(branching)

  /** Methods that are implicitly added to container combinations that look like (nested) collections. */
  class CollectionMethods(collection: Collection)

}

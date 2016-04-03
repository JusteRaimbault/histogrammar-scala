package org.dianahep

import scala.language.implicitConversions

import org.dianahep.histogrammar.json._

package object histogrammar {
  implicit def toWeighted[DATUM](datum: DATUM) = Weighted(datum)
  implicit def domainToWeighted[DOMAIN, RANGE](f: DOMAIN => RANGE) = {x: Weighted[DOMAIN] => f(x.datum)}

  implicit def booleanToSelection[DATUM](f: DATUM => Boolean) = Selection({x: Weighted[DATUM] => if (f(x.datum)) 1.0 else 0.0})
  implicit def byteToSelection[DATUM](f: DATUM => Byte) = Selection({x: Weighted[DATUM] => f(x.datum).toDouble})
  implicit def shortToSelection[DATUM](f: DATUM => Short) = Selection({x: Weighted[DATUM] => f(x.datum).toDouble})
  implicit def intToSelection[DATUM](f: DATUM => Int) = Selection({x: Weighted[DATUM] => f(x.datum).toDouble})
  implicit def longToSelection[DATUM](f: DATUM => Long) = Selection({x: Weighted[DATUM] => f(x.datum).toDouble})
  implicit def floatToSelection[DATUM](f: DATUM => Float) = Selection({x: Weighted[DATUM] => f(x.datum).toDouble})
  implicit def doubleToSelection[DATUM](f: DATUM => Double) = Selection({x: Weighted[DATUM] => f(x.datum)})
  implicit def weightedBooleanToSelection[DATUM](f: Weighted[DATUM] => Boolean) = Selection({x: Weighted[DATUM] => if (f(x)) 1.0 else 0.0})
  implicit def weightedByteToSelection[DATUM](f: Weighted[DATUM] => Byte) = Selection({x: Weighted[DATUM] => f(x).toDouble})
  implicit def weightedShortToSelection[DATUM](f: Weighted[DATUM] => Short) = Selection({x: Weighted[DATUM] => f(x).toDouble})
  implicit def weightedIntToSelection[DATUM](f: Weighted[DATUM] => Int) = Selection({x: Weighted[DATUM] => f(x).toDouble})
  implicit def weightedLongToSelection[DATUM](f: Weighted[DATUM] => Long) = Selection({x: Weighted[DATUM] => f(x).toDouble})
  implicit def weightedFloatToSelection[DATUM](f: Weighted[DATUM] => Float) = Selection({x: Weighted[DATUM] => f(x).toDouble})
  implicit def weightedDoubleToSelection[DATUM](f: Weighted[DATUM] => Double) = Selection(f)

  type NumericalFcn[DATUM] = Weighted[DATUM] => Double
  type NumericalFcn2D[DATUM] = Weighted[DATUM] => (Double, Double)
  type NumericalFcn3D[DATUM] = Weighted[DATUM] => (Double, Double, Double)
  type NumericalFcnND[DATUM] = Weighted[DATUM] => Seq[Double]
  type CategoricalFcn[DATUM] = Weighted[DATUM] => String

  implicit def unweighted[DATUM] = Selection({x: Weighted[DATUM] => 1.0})

  ContainerFactory.register(Counted)
  ContainerFactory.register(Summed)
  ContainerFactory.register(Averaged)
  ContainerFactory.register(Deviated)
  ContainerFactory.register(Binned)
  ContainerFactory.register(Mapped)
  ContainerFactory.register(Branched)

  implicit def tupleToBranched2[C1 <: Container[C1], C2 <: Container[C2]](x: Tuple2[C1, C2]) = new Branched2(x._1, x._2)
  implicit def tupleToBranched3[C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](x: Tuple3[C1, C2, C3]) = new Branched3(x._1, x._2, x._3)

  implicit def tupleToBranching2[DATUM, C1 <: Container[C1], C2 <: Container[C2]](x: Tuple2[Aggregator[DATUM, C1], Aggregator[DATUM, C2]]) = new Branching2(unweighted[DATUM], x._1, x._2)
  implicit def tupleToBranching3[DATUM, C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](x: Tuple3[Aggregator[DATUM, C1], Aggregator[DATUM, C2], Aggregator[DATUM, C3]]) = new Branching3(unweighted[DATUM], x._1, x._2, x._3)
}

package histogrammar {
  //////////////////////////////////////////////////////////////// data model (user's data are implicitly converted to this)

  case class Weighted[DATUM](datum: DATUM, weight: Double = 1.0) {
    def reweight(w: Double): Weighted[DATUM] = copy(weight = weight*w)
    def contributes = weight > 0.0
  }

  case class Selection[DATUM](f: Weighted[DATUM] => Double) extends Function1[Weighted[DATUM], Double] {
    def apply(x: Weighted[DATUM]) = f(x)
  }

  class AggregatorException(message: String, cause: Exception = null) extends Exception(message, cause)

  //////////////////////////////////////////////////////////////// general definition of an container/aggregator

  // creates containers (from arguments or JSON)
  trait ContainerFactory {
    def name: String
    def fromJsonFragment(json: Json): Container[_]
  }
  object ContainerFactory {
    private var known = Map[String, ContainerFactory]()

    def register(factory: ContainerFactory) {
      known = known.updated(factory.name, factory)
    }

    def apply(name: String) = known.get(name) match {
      case Some(x) => x
      case None => throw new AggregatorException(s"unrecognized aggregator (is it a custom aggregator that hasn't been registered?): $name")
    }

    def fromJson[CONTAINER <: Container[CONTAINER]](str: String): CONTAINER = Json.parse(str) match {
      case Some(json) => fromJson(json).asInstanceOf[CONTAINER]
      case None => throw new InvalidJsonException(str)
    }

    def fromJson[CONTAINER <: Container[CONTAINER]](json: Json): CONTAINER = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("type", "data")) =>
        val get = pairs.toMap

        val name = get("type") match {
          case JsonString(x) => x
          case x => throw new JsonFormatException(x, "type")
        }

        ContainerFactory(name).fromJsonFragment(get("data")).asInstanceOf[CONTAINER]

      case _ => throw new JsonFormatException(json, "ContainerFactory")
    }
  }

  // immutable container of data; the result of an aggregation
  trait Container[CONTAINER <: Container[CONTAINER]] extends Serializable {
    def factory: ContainerFactory

    def +(that: CONTAINER): CONTAINER

    def toJson: Json = JsonObject("type" -> JsonString(factory.name), "data" -> toJsonFragment)
    def toJsonFragment: Json
  }

  // creates aggregators (from arguments)
  trait AggregatorFactory

  // mutable aggregator of data; produces a container
  trait Aggregator[DATUM, CONTAINER <: Container[CONTAINER]] extends Container[CONTAINER] {
    def selection: Selection[DATUM]

    def fill(x: Weighted[DATUM])

    def fix: CONTAINER

    // satisfy Container[CONTAINER] contract by passing everything through fix
    def factory = fix.factory
    def +(that: CONTAINER) = fix.+(that)
    def +(that: Aggregator[DATUM, CONTAINER]) = fix.+(that.fix)
    def toJsonFragment = fix.toJsonFragment
  }

  //////////////////////////////////////////////////////////////// Counted/Counting

  object Counted extends ContainerFactory {
    val name = "Counted"

    def apply(value: Double) = new Counted(value)
    def unapply(x: Counted) = Some(x.value)

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonFloat(value) => new Counted(value)
      case _ => throw new JsonFormatException(json, "Counted")
    }
  }
  class Counted(val value: Double) extends Container[Counted] {
    def factory = Counted

    def +(that: Counted) = new Counted(this.value + that.value)

    def toJsonFragment = JsonFloat(value)
    override def toString() = s"Counted"
    override def equals(that: Any) = that match {
      case that: Counted => this.value == that.value
      case _ => false
    }
    override def hashCode() = value.hashCode
  }

  object Counting extends AggregatorFactory {
    def apply[DATUM](selection: Selection[DATUM] = unweighted[DATUM]) = new Counting(selection, 0.0)
    def unapply(x: Counting[_]) = Some(x.value)
  }
  class Counting[DATUM](val selection: Selection[DATUM], var value: Double) extends Aggregator[DATUM, Counted] {
    def fill(x: Weighted[DATUM]) {
      val y = x reweight selection(x)
      value += y.weight
    }
    def fix = new Counted(value)
    override def toString() = s"Counting"
    override def equals(that: Any) = that match {
      case that: Counting[DATUM] => this.selection == that.selection  &&  this.value == that.value
      case _ => false
    }
    override def hashCode() = (selection, value).hashCode
  }

  //////////////////////////////////////////////////////////////// Summed/Summing

  object Summed extends ContainerFactory {
    val name = "Summed"

    def apply(value: Double) = new Summed(value)
    def unapply(x: Summed) = Some(x.value)

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonFloat(value) => new Summed(value)
      case _ => throw new JsonFormatException(json, "Summed")
    }
  }
  class Summed(val value: Double) extends Container[Summed] {
    def factory = Summed

    def +(that: Summed) = new Summed(this.value + that.value)

    def toJsonFragment = JsonFloat(value)
    override def toString() = s"Summed"
    override def equals(that: Any) = that match {
      case that: Summed => this.value == that.value
      case _ => false
    }
    override def hashCode() = value.hashCode
  }

  object Summing extends AggregatorFactory {
    def apply[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = new Summing(quantity, selection, 0.0)
    def unapply(x: Summing[_]) = Some(x.value)
  }
  class Summing[DATUM](val quantity: NumericalFcn[DATUM], val selection: Selection[DATUM], var value: Double) extends Aggregator[DATUM, Summed] {
    def fill(x: Weighted[DATUM]) {
      val y = quantity(x) reweight selection(x)
      value += y.datum * y.weight
    }
    def fix = new Summed(value)
    override def toString() = s"Summing"
    override def equals(that: Any) = that match {
      case that: Summing[DATUM] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.value == that.value
      case _ => false
    }
    override def hashCode() = (quantity, selection, value).hashCode
  }

  //////////////////////////////////////////////////////////////// Averaged/Averaging

  object Averaged extends ContainerFactory {
    val name = "Averaged"

    def apply(count: Double, mean: Double) = new Averaged(count, mean)
    def unapply(x: Averaged) = Some((x.count, x.mean))

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("count", "mean")) =>
        val get = pairs.toMap

        val count = get("count") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Averaged.count")
        }

        val mean = get("mean") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Averaged.mean")
        }

        new Averaged(count, mean)

      case _ => throw new JsonFormatException(json, "Averaged")
    }
  }
  class Averaged(val count: Double, val mean: Double) extends Container[Averaged] {
    def factory = Averaged

    def +(that: Averaged) = new Averaged(
      this.count + that.count,
      (this.mean*this.count + that.mean*that.count) / (this.count + that.count))

    def toJsonFragment = JsonObject("count" -> JsonFloat(count), "mean" -> JsonFloat(mean))
    override def toString() = s"Averaged"
    override def equals(that: Any) = that match {
      case that: Averaged => this.count == that.count  &&  this.mean == that.mean
      case _ => false
    }
    override def hashCode() = (count, mean).hashCode
  }

  object Averaging extends AggregatorFactory {
    def apply[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = new Averaging(quantity, selection, 0.0, 0.0)
    def unapply(x: Averaging[_]) = Some((x.count, x.mean))
  }
  class Averaging[DATUM](val quantity: NumericalFcn[DATUM], val selection: Selection[DATUM], var count: Double, var mean: Double) extends Aggregator[DATUM, Averaged] {
    def fill(x: Weighted[DATUM]) {
      val y = quantity(x) reweight selection(x)

      if (y.contributes) {
        count += y.weight

        val delta = y.datum - mean
        val shift = delta * y.weight / count

        mean += shift
      }
    }

    def fix = new Averaged(count, mean)
    override def toString() = s"Averaging"
    override def equals(that: Any) = that match {
      case that: Averaging[DATUM] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.count == that.count  &&  this.mean == that.mean
      case _ => false
    }
    override def hashCode() = (quantity, selection, count, mean).hashCode
  }

  //////////////////////////////////////////////////////////////// Deviated/Deviating

  object Deviated extends ContainerFactory {
    val name = "Deviated"

    def apply(count: Double, mean: Double, variance: Double) = new Deviated(count, mean, variance)
    def unapply(x: Deviated) = Some((x.count, x.mean, x.variance))

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("count", "mean", "variance")) =>
        val get = pairs.toMap

        val count = get("count") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Deviated.count")
        }

        val mean = get("mean") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Deviated.mean")
        }

        val variance = get("variance") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Deviated.variance")
        }

        new Deviated(count, mean, variance)

      case _ => throw new JsonFormatException(json, "Deviated")
    }
  }
  class Deviated(val count: Double, val mean: Double, val variance: Double) extends Container[Deviated] {
    def factory = Deviated

    def +(that: Deviated) = {
      val ca = this.count
      val cb = that.count
      val mua = this.mean
      val mub = that.mean
      val sa = this.variance * this.count
      val sb = that.variance * that.count

      val muab = (ca*mua + cb*mub) / (ca + cb)
      val sab = sa + sb + ca*mua*mua + cb*mub*mub - 2.0*muab*(ca*mua + cb*mub) + muab*muab*(ca + cb)

      new Deviated(ca * cb, muab, sab / (ca + cb))
    }

    def toJsonFragment = JsonObject("count" -> JsonFloat(count), "mean" -> JsonFloat(mean), "variance" -> JsonFloat(variance))
    override def toString() = s"Deviated"
    override def equals(that: Any) = that match {
      case that: Deviated => this.count == that.count  &&  this.mean == that.mean  &&  this.variance == that.variance
      case _ => false
    }
    override def hashCode() = (count, mean, variance).hashCode
  }

  object Deviating extends AggregatorFactory {
    def apply[DATUM](quantity: NumericalFcn[DATUM], selection: Selection[DATUM] = unweighted[DATUM]) = new Deviating(quantity, selection, 0.0, 0.0, 0.0)
    def unapply(x: Deviating[_]) = Some((x.count, x.mean, x.variance))
  }
  class Deviating[DATUM](val quantity: NumericalFcn[DATUM], val selection: Selection[DATUM], var count: Double, var mean: Double, var variance: Double) extends Aggregator[DATUM, Deviated] {
    def fill(x: Weighted[DATUM]) {
      val y = quantity(x) reweight selection(x)

      if (y.contributes) {
        val originalCount = count

        count += y.weight

        val delta = y.datum - mean
        val shift = delta * y.weight / count
        val varianceTimesCount = originalCount * (variance + delta * shift)

        mean += shift
        variance = varianceTimesCount / count
      }
    }
    def fix = new Deviated(count, mean, variance)
    override def toString() = s"Deviating"
    override def equals(that: Any) = that match {
      case that: Deviating[DATUM] => this.quantity == that.quantity  &&  this.selection == that.selection  &&  this.count == that.count  &&  this.mean == that.mean  &&  this.variance == that.variance
      case _ => false
    }
    override def hashCode() = (quantity, selection, count, mean, variance).hashCode
  }

  //////////////////////////////////////////////////////////////// Binned/Binning

  object Binned extends ContainerFactory {
    val name = "Binned"

    def apply[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]]
      (low: Double,
        high: Double)
      (values: Seq[V],
        underflow: U,
        overflow: O,
        nanflow: N) =
      new Binned[V, U, O, N](low, high, values, underflow, overflow, nanflow)

    def unapply[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]](x: Binned[V, U, O, N]) = Some((x.values, x.underflow, x.overflow, x.nanflow))

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) if (pairs.keySet == Set("low", "high", "values:type", "values", "underflow:type", "underflow", "overflow:type", "overflow", "nanflow:type", "nanflow")) =>
        val get = pairs.toMap

        val low = get("low") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Binned.low")
        }

        val high = get("high") match {
          case JsonNumber(x) => x
          case x => throw new JsonFormatException(x, "Binned.high")
        }

        val valuesFactory = get("values:type") match {
          case JsonString(name) => ContainerFactory(name)
          case x => throw new JsonFormatException(x, "Binned.values:type")
        }
        val values = get("values") match {
          case JsonArray(sub @ _*) => sub.map(valuesFactory.fromJsonFragment(_))
          case x => throw new JsonFormatException(x, "Binned.values")
        }

        val underflowFactory = get("underflow:type") match {
          case JsonString(name) => ContainerFactory(name)
          case x => throw new JsonFormatException(x, "Binned.underflow:type")
        }
        val underflow = underflowFactory.fromJsonFragment(get("underflow"))

        val overflowFactory = get("overflow:type") match {
          case JsonString(name) => ContainerFactory(name)
          case x => throw new JsonFormatException(x, "Binned.overflow:type")
        }
        val overflow = overflowFactory.fromJsonFragment(get("overflow"))

        val nanflowFactory = get("nanflow:type") match {
          case JsonString(name) => ContainerFactory(name)
          case x => throw new JsonFormatException(x, "Binned.nanflow:type")
        }
        val nanflow = nanflowFactory.fromJsonFragment(get("nanflow"))

        new Binned[Container[_], Container[_], Container[_], Container[_]](low, high, values, underflow, overflow, nanflow)

      case _ => throw new JsonFormatException(json, "Binned")
    }
  }
  class Binned[V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]](
    val low: Double,
    val high: Double,
    val values: Seq[V],
    val underflow: U,
    val overflow: O,
    val nanflow: N) extends Container[Binned[V, U, O, N]] {

    if (low >= high)
      throw new AggregatorException(s"low ($low) must be less than high ($high)")
    if (values.size < 1)
      throw new AggregatorException(s"values ($values) must have at least one element")

    def factory = Binned

    def +(that: Binned[V, U, O, N]) = {
      if (this.low != that.low)
        throw new AggregatorException(s"cannot add Binned because low differs (${this.low} vs ${that.low})")
      if (this.high != that.high)
        throw new AggregatorException(s"cannot add Binned because high differs (${this.high} vs ${that.high})")
      if (this.values.size != that.values.size)
        throw new AggregatorException(s"cannot add Binned because number of values differs (${this.values.size} vs ${that.values.size})")
      if (this.values.isEmpty)
        throw new AggregatorException(s"cannot add Binned because number of values is zero")
      if (this.values.head.factory != that.values.head.factory)
        throw new AggregatorException(s"cannot add Binned because values type differs (${this.values.head.factory.name} vs ${that.values.head.factory.name})")
      if (this.underflow.factory != that.underflow.factory)
        throw new AggregatorException(s"cannot add Binned because underflow type differs (${this.underflow.factory.name} vs ${that.underflow.factory.name})")
      if (this.overflow.factory != that.overflow.factory)
        throw new AggregatorException(s"cannot add Binned because overflow type differs (${this.overflow.factory.name} vs ${that.overflow.factory.name})")
      if (this.nanflow.factory != that.nanflow.factory)
        throw new AggregatorException(s"cannot add Binned because nanflow type differs (${this.nanflow.factory.name} vs ${that.nanflow.factory.name})")

      new Binned(
        low,
        high,
        this.values zip that.values map {case (me, you) => me + you},
        this.underflow + that.underflow,
        this.overflow + that.overflow,
        this.nanflow + that.nanflow)
    }

    def toJsonFragment = JsonObject(
      "low" -> JsonFloat(low),
      "high" -> JsonFloat(high),
      "values:type" -> JsonString(values.head.factory.name),
      "values" -> JsonArray(values.map(_.toJsonFragment): _*),
      "underflow:type" -> JsonString(underflow.factory.name),
      "underflow" -> underflow.toJsonFragment,
      "overflow:type" -> JsonString(overflow.factory.name),
      "overflow" -> overflow.toJsonFragment,
      "nanflow:type" -> JsonString(nanflow.factory.name),
      "nanflow" -> nanflow.toJsonFragment)

    override def toString() = s"Binned[low=$low, high=$high, values=[${values.head.toString}, size=${values.size}], underflow=$underflow, overflow=$overflow, nanflow=$nanflow]"
    override def equals(that: Any) = that match {
      case that: Binned[V, U, O, N] => this.low == that.low  &&  this.high == that.high  &&  this.values == that.values  &&  this.underflow == that.underflow  &&  this.overflow == that.overflow  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (low, high, values, underflow, overflow, nanflow).hashCode
  }

  object Binning extends AggregatorFactory {
    def apply[DATUM, V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]]
      (num: Int,
        low: Double,
        high: Double,
        key: NumericalFcn[DATUM],
        selection: Selection[DATUM] = unweighted[DATUM])
      (value: => Aggregator[DATUM, V] = Counting[DATUM](),
        underflow: Aggregator[DATUM, U] = Counting[DATUM](),
        overflow: Aggregator[DATUM, O] = Counting[DATUM](),
        nanflow: Aggregator[DATUM, N] = Counting[DATUM]()) =
      new Binning[DATUM, V, U, O, N](low, high, key, selection, Array.fill(num)(value).toSeq, underflow, overflow, nanflow)

    def unapply[DATUM, V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]](x: Binning[DATUM, V, U, O, N]) = Some((x.values, x.underflow, x.overflow, x.nanflow))
  }
  class Binning[DATUM, V <: Container[V], U <: Container[U], O <: Container[O], N <: Container[N]](
    val low: Double,
    val high: Double,
    val key: NumericalFcn[DATUM],
    val selection: Selection[DATUM],
    val values: Seq[Aggregator[DATUM, V]],
    val underflow: Aggregator[DATUM, U],
    val overflow: Aggregator[DATUM, O],
    val nanflow: Aggregator[DATUM, N]) extends Aggregator[DATUM, Binned[V, U, O, N]] {

    if (low >= high)
      throw new AggregatorException(s"low ($low) must be less than high ($high)")
    if (values.size < 1)
      throw new AggregatorException(s"values ($values) must have at least one element")

    def bin(k: Double): Int =
      if (under(k)  ||  over(k)  ||  nan(k))
        -1
      else
        Math.floor(values.size * (k - low) / (high - low)).toInt

    def under(k: Double): Boolean = !k.isNaN  &&  k < low
    def over(k: Double): Boolean = !k.isNaN  &&  k >= high
    def nan(k: Double): Boolean = k.isNaN

    def fill(x: Weighted[DATUM]) {
      val k = key(x)
      val y = x reweight selection(x)
      if (y.contributes) {
        if (under(k))
          underflow.fill(y)
        else if (over(k))
          overflow.fill(y)
        else if (nan(k))
          nanflow.fill(y)
        else
          values(bin(k)).fill(y)
      }
    }

    def fix = new Binned(low, high, values.map(_.fix), underflow.fix, overflow.fix, nanflow.fix)
    override def toString() = s"Binning[low=$low, high=$high, values=[${values.head.toString}, size=${values.size}], underflow=$underflow, overflow=$overflow, nanflow=$nanflow]"
    override def equals(that: Any) = that match {
      case that: Binning[DATUM, V, U, O, N] => this.low == that.low  &&  this.high == that.high  &&  this.key == that.key  &&  this.selection == that.selection  &&  this.values == that.values  &&  this.underflow == that.underflow  &&  this.overflow == that.overflow  &&  this.nanflow == that.nanflow
      case _ => false
    }
    override def hashCode() = (low, high, key, selection, values, underflow, overflow, nanflow).hashCode
  }

  //////////////////////////////////////////////////////////////// Mapped/Mapping

  // this is a *heterogeneous* map, so some runtime casting is necessary; also note that data is broadcast to all members
  object Mapped extends ContainerFactory {
    val name = "Mapped"

    def apply(pairs: (String, Container[_])*) = new Mapped(pairs: _*)
    def unapplySeq(x: Mapped) = Some(x.pairs)

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonObject(pairs @ _*) =>
        new Mapped(pairs map {
          case (JsonString(key), JsonObject(typedata @ _*)) if (typedata.keySet == Set("type", "data")) =>
            val get = typedata.toMap
            (get("type"), get("data")) match {
              case (JsonString(factory), sub) => (key.toString, ContainerFactory(factory).fromJsonFragment(sub))
              case _ => throw new JsonFormatException(json, s"""Mapped key "$key"""")
            }
          case _ => throw new JsonFormatException(json, s"Mapped key")
        }: _*)

      case _ => throw new JsonFormatException(json, "Mapped")
    }
  }
  class Mapped(val pairs: (String, Container[_])*) extends Container[Mapped] {
    def factory = Mapped

    val pairsMap = pairs.toMap
    def keys: Iterable[String] = pairs.toIterable.map(_._1)
    def values: Iterable[Container[_]] = pairs.toIterable.map(_._2)
    def keySet: Set[String] = keys.toSet
    def apply[CONTAINER <: Container[CONTAINER]](x: String) = pairsMap(x).asInstanceOf[CONTAINER]
    def get[CONTAINER <: Container[CONTAINER]](x: String) = pairsMap.get(x).asInstanceOf[CONTAINER]
    def getOrElse[CONTAINER <: Container[CONTAINER]](x: String, default: => CONTAINER) = pairsMap.getOrElse(x, default).asInstanceOf[CONTAINER]

    private def combine[CONTAINER <: Container[CONTAINER]](one: Container[_], two: Container[_]) =
      one.asInstanceOf[CONTAINER] + two.asInstanceOf[CONTAINER]

    def +(that: Mapped) =
      if (this.keySet != that.keySet)
        throw new AggregatorException(s"""cannot add Mapped because they have different keys:\n    ${this.keys.toArray.sorted.mkString(" ")}\nvs\n    ${that.keys.toArray.sorted.mkString(" ")}""")
      else
        new Mapped(this.pairs.map({case (key, mysub) =>
          val yoursub = that.pairsMap(key)
          if (mysub.factory != yoursub.factory)
            throw new AggregatorException(s"""cannot add Mapped because key "$key" has a different type in the two maps: ${mysub.factory.name} vs ${yoursub.factory.name}""")
          (key, combine(mysub, yoursub))
        }): _*)

    def toJsonFragment = JsonObject(pairs map {case (key, sub) =>
      key -> JsonObject("type" -> JsonString(sub.factory.name), "data" -> sub.toJsonFragment)
    }: _*)

    override def toString() = s"Mapped[size=${pairs.size}]"
    override def equals(that: Any) = that match {
      case that: Mapped => this.pairsMap == that.pairsMap
      case _ => false
    }
    override def hashCode() = pairsMap.hashCode
  }

  object Mapping extends AggregatorFactory {
    def apply[DATUM](pairs: (String, Aggregator[DATUM, _])*)(implicit selection: Selection[DATUM] = unweighted[DATUM]) = new Mapping(selection, pairs: _*)
    def unapplySeq[DATUM](x: Mapping[DATUM]) = Some(x.pairs)
  }
  class Mapping[DATUM](val selection: Selection[DATUM], val pairs: (String, Aggregator[DATUM, _])*) extends Aggregator[DATUM, Mapped] {
    val pairsMap = pairs.toMap
    def keys: Iterable[String] = pairs.toIterable.map(_._1)
    def values: Iterable[Aggregator[DATUM, _]] = pairs.toIterable.map(_._2)
    def keySet: Set[String] = keys.toSet
    def apply[AGGREGATOR <: Aggregator[DATUM, _]](x: String) = pairsMap(x).asInstanceOf[AGGREGATOR]
    def get[AGGREGATOR <: Aggregator[DATUM, _]](x: String) = pairsMap.get(x).asInstanceOf[AGGREGATOR]
    def getOrElse[AGGREGATOR <: Aggregator[DATUM, _]](x: String, default: => AGGREGATOR) = pairsMap.getOrElse(x, default).asInstanceOf[AGGREGATOR]

    def fill(x: Weighted[DATUM]) {
      val y = x reweight selection(x)
      if (y.contributes)
        values.foreach(_.fill(x))
    }

    def fix = new Mapped(pairs map {case (key, sub) => (key, sub.fix.asInstanceOf[Container[_]])}: _*)
    override def toString() = s"Mapping[size=${pairs.size}]"
    override def equals(that: Any) = that match {
      case that: Mapping[DATUM] => this.selection == that.selection  &&  this.pairsMap == that.pairsMap
      case _ => false
    }
    override def hashCode() = (selection, pairsMap).hashCode
  }

  //////////////////////////////////////////////////////////////// Branched/Branching

  // this is a *type-safe* tuple, with explicit cases for 2 through ?? branches
  object Branched extends ContainerFactory {
    val name = "Branched"

    def apply[C1 <: Container[C1], C2 <: Container[C2]](_1: C1, _2: C2) = new Branched2[C1, C2](_1, _2)
    def apply[C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](_1: C1, _2: C2, _3: C3) = new Branched3[C1, C2, C3](_1, _2, _3)

    def unapply[C1 <: Container[C1], C2 <: Container[C2]](x: Branched2[C1, C2]) = Some((x._1, x._2))
    def unapply[C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](x: Branched3[C1, C2, C3]) = Some((x._1, x._2, x._3))

    def fromJsonFragment(json: Json): Container[_] = json match {
      case JsonArray(elements @ _*) if (elements.size >= 2) =>
        val subs = elements.zipWithIndex map {
          case (JsonObject(pairs @ _*), i) if (pairs.keySet == Set("type", "data")) =>
            val get = pairs.toMap
            (get("type"), get("data")) match {
              case (JsonString(factory), sub) => ContainerFactory(factory).fromJsonFragment(sub)
              case _ => throw new JsonFormatException(json, s"Branched item $i")
            }
          case _ => throw new JsonFormatException(json, "Branched item")
        }
        if (subs.size == 2) new Branched2[Container[_], Container[_]](subs(0), subs(1)).asInstanceOf[Container[_]]
        else if (subs.size == 3) new Branched3[Container[_], Container[_], Container[_]](subs(0), subs(1), subs(2)).asInstanceOf[Container[_]]
        else
          throw new JsonFormatException(json, "Branched (too many branches)")

      case _ => throw new JsonFormatException(json, "Branched")
    }

    private[histogrammar] def typedata[CONTAINER <: Container[CONTAINER]](sub: CONTAINER) = JsonObject("type" -> JsonString(sub.factory.name), "data" -> sub.toJsonFragment)
  }
  class Branched2[C1 <: Container[C1], C2 <: Container[C2]](_1: C1, _2: C2) extends Tuple2[C1, C2](_1, _2) with Container[Branched2[C1, C2]] {
    def factory = Branched
    def +(that: Branched2[C1, C2]) = new Branched2[C1, C2](this._1 + that._1, this._2 + that._2)
    def toJsonFragment = JsonArray(Branched.typedata(_1), Branched.typedata(_2))
    override def toString = s"""Branched2[${_1}, ${_2}]"""
  }
  class Branched3[C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](_1: C1, _2: C2, _3: C3) extends Tuple3[C1, C2, C3](_1, _2, _3) with Container[Branched3[C1, C2, C3]] {
    def factory = Branched
    def +(that: Branched3[C1, C2, C3]) = new Branched3[C1, C2, C3](this._1 + that._1, this._2 + that._2, this._3 + that._3)
    def toJsonFragment = JsonArray(Branched.typedata(_1), Branched.typedata(_2), Branched.typedata(_3))
    override def toString = s"""Branched3[${_1}, ${_2}, ${_3}]"""
  }

  object Branching extends AggregatorFactory {
    def apply[DATUM, C1 <: Container[C1], C2 <: Container[C2]](_1: Aggregator[DATUM, C1], _2: Aggregator[DATUM, C2])(implicit selection: Selection[DATUM] = unweighted[DATUM]) = new Branching2[DATUM, C1, C2](selection, _1, _2)
    def apply[DATUM, C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](_1: Aggregator[DATUM, C1], _2: Aggregator[DATUM, C2], _3: Aggregator[DATUM, C3])(implicit selection: Selection[DATUM] = unweighted[DATUM]) = new Branching3[DATUM, C1, C2, C3](selection, _1, _2, _3)

    def unapply[C1 <: Container[C1], C2 <: Container[C2]](x: Branched2[C1, C2]) = Some((x._1, x._2))
    def unapply[C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](x: Branched3[C1, C2, C3]) = Some((x._1, x._2, x._3))
  }
  class Branching2[DATUM, C1 <: Container[C1], C2 <: Container[C2]](val selection: Selection[DATUM], _1: Aggregator[DATUM, C1], _2: Aggregator[DATUM, C2]) extends Tuple2[Aggregator[DATUM, C1], Aggregator[DATUM, C2]](_1, _2) with Aggregator[DATUM, Branched2[C1, C2]] {
    def fill(x: Weighted[DATUM]) {
      val y = x reweight selection(x)
      if (y.contributes) {
        _1.fill(x)
        _2.fill(x)
      }
    }
    def fix = new Branched2[C1, C2](_1.fix, _2.fix)
    override def toString = s"""Branching2[${_1}, ${_2}]"""
  }
  class Branching3[DATUM, C1 <: Container[C1], C2 <: Container[C2], C3 <: Container[C3]](val selection: Selection[DATUM], _1: Aggregator[DATUM, C1], _2: Aggregator[DATUM, C2], _3: Aggregator[DATUM, C3]) extends Tuple3[Aggregator[DATUM, C1], Aggregator[DATUM, C2], Aggregator[DATUM, C3]](_1, _2, _3) with Aggregator[DATUM, Branched3[C1, C2, C3]] {
    def fill(x: Weighted[DATUM]) {
      val y = x reweight selection(x)
      if (y.contributes) {
        _1.fill(x)
        _2.fill(x)
        _3.fill(x)
      }
    }
    def fix = new Branched3[C1, C2, C3](_1.fix, _2.fix, _3.fix)
    override def toString = s"""Branching3[${_1}, ${_2}, ${_3}]"""
  }

}

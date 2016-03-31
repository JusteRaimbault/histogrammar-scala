package org.dianahep.histogrammar

import scala.language.implicitConversions

package object json {
  implicit def booleanToJson(x: Boolean) = if (x) JsonTrue else JsonFalse
  implicit def byteToJson(x: Byte) = JsonInt(x)
  implicit def shortToJson(x: Short) = JsonInt(x)
  implicit def intToJson(x: Int) = JsonInt(x)
  implicit def longToJson(x: Long) = JsonInt(x)
  implicit def floatToJson(x: Float) = JsonFloat(x)
  implicit def doubleToJson(x: Double) = JsonFloat(x)
  implicit def charToJson(x: Char) = JsonString(x.toString)
  implicit def stringToJson(x: String) = JsonString(x)

  implicit class HasKeySet(pairs: Seq[(JsonString, Json)]) {
    def keySet: Set[String] = pairs.map(_._1.value).toSet
  }

  private[json] def whitespace(p: ParseState) {
    while (p.remaining >= 1  &&  (p.get == ' '  ||  p.get == '\t'  ||  p.get == '\n'  ||  p.get == '\r'))
      p.update(1)
  }

  private[json] def parseFully[X <: Json](str: String, parser: ParseState => Option[X]) = {
    val p = ParseState(str)
    whitespace(p)
    val out = parser(p)
    whitespace(p)
    if (p.done)
      out.asInstanceOf[Option[X]]
    else
      None
  }
}

package json {
  case class ParseState(str: String, var pos: Int = 0) {
    private var stack = List[Int]()
    def save() {
      stack = pos :: stack
    }
    def restore() {
      pos = stack.head
      stack = stack.tail
    }
    def unsave() {
      stack = stack.tail
    }
    def get = str(pos)
    def get(n: Int) = str.substring(pos, pos + n)
    def update(n: Int) = {pos += n}
    def remaining = str.size - pos
    def done = str.size == pos
  }

  sealed trait Json
  object Json {
    def parse(str: String): Option[Json] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[Json] =
      (JsonNull.parse(p) orElse
        JsonTrue.parse(p) orElse
        JsonFalse.parse(p) orElse
        JsonNumber.parse(p) orElse
        JsonString.parse(p) orElse
        JsonArray.parse(p) orElse
        JsonObject.parse(p))
  }

  trait JsonPrimitive extends Json
  object JsonPrimitive {
    def parse(str: String): Option[Json] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[Json] =
      (JsonNull.parse(p) orElse
        JsonTrue.parse(p) orElse
        JsonFalse.parse(p) orElse
        JsonNumber.parse(p) orElse
        JsonString.parse(p))
  }

  case object JsonNull extends JsonPrimitive {
    def stringify = "null"
    def parse(str: String): Option[JsonNull.type] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonNull.type] =
      if (p.remaining >= 4  &&  p.get(4) == "null") {
        p.update(4)
        Some(JsonNull)
      }
      else
        None
  }

  trait JsonBoolean extends JsonPrimitive
  object JsonBoolean {
    def unapply(x: Json): Option[Boolean] = x match {
      case JsonTrue => Some(true)
      case JsonFalse => Some(false)
      case _ => None
    }
    def parse(str: String): Option[JsonBoolean] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonBoolean] = JsonTrue.parse(p) orElse JsonFalse.parse(p)
  }

  case object JsonTrue extends JsonBoolean {
    def stringify = "true"
    def parse(str: String): Option[JsonTrue.type] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonTrue.type] =
      if (p.remaining >= 4  &&  p.get(4) == "true") {
        p.update(4)
        Some(JsonTrue)
      }
      else
        None
  }

  case object JsonFalse extends JsonBoolean {
    def stringify = "false"
    def parse(str: String): Option[JsonFalse.type] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonFalse.type] =
      if (p.remaining >= 5  &&  p.get(5) == "false") {
        p.update(5)
        Some(JsonFalse)
      }
      else
        None
  }

  trait JsonNumber extends JsonPrimitive {
    def toChar: Char
    def toByte: Byte
    def toShort: Short
    def toInt: Int
    def toLong: Long
    def toFloat: Float
    def toDouble: Double
  }
  object JsonNumber {
    def unapply(x: Json): Option[Double] = x match {
      case JsonInt(y) => Some(y.toDouble)
      case JsonFloat(y) => Some(y)
      case _ => None
    }

    def parse(str: String): Option[JsonNumber] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonNumber] =
      if (!p.done  &&  (('0' <= p.get  &&  p.get <= '9')  ||  p.get == '-')) {
        p.save()
        val sb = new java.lang.StringBuilder
        while (!p.done  &&  (('0' <= p.get  &&  p.get <= '9')  ||  p.get == '-'  ||  p.get == '+'  ||  p.get == 'e'  ||  p.get == 'E'  ||  p.get == '.')) {
          sb.append(p.get)
          p.update(1)
        }
        val str = sb.toString
        try {
          val out = Some(JsonInt(java.lang.Long.parseLong(str)))
          p.unsave()
          out
        }
        catch {
          case _: java.lang.NumberFormatException =>
            try {
              val out = Some(JsonFloat(java.lang.Double.parseDouble(str)))
              p.unsave()
              out
            }
            catch {
              case _: java.lang.NumberFormatException =>
                p.restore()
                None
            }
        }
      }
      else
        None
  }

  case class JsonInt(value: Long) extends JsonNumber {
    def stringify = value.toString
    def toChar = value.toChar
    def toByte = value.toByte
    def toShort = value.toShort
    def toInt = value.toInt
    def toLong = value.toLong
    def toFloat = value.toFloat
    def toDouble = value.toDouble
  }
  object JsonInt {
    def parse(str: String): Option[JsonInt] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonInt] = JsonNumber.parse(p) match {
      case Some(JsonInt(x)) => Some(JsonInt(x))
      case _ => None
    }
  }

  case class JsonFloat(value: Double) extends JsonNumber {
    def stringify = value.toString
    def toChar = value.toChar
    def toByte = value.toByte
    def toShort = value.toShort
    def toInt = value.toInt
    def toLong = value.toLong
    def toFloat = value.toFloat
    def toDouble = value.toDouble
  }
  object JsonFloat {
    def parse(str: String): Option[JsonFloat] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonFloat] = JsonNumber.parse(p) match {
      case Some(JsonInt(x)) => Some(JsonFloat(x))
      case Some(JsonFloat(x)) => Some(JsonFloat(x))
      case _ => None
    }
  }

  case class JsonString(value: String) extends JsonPrimitive {
    override def toString() = "JsonString(" + stringify + ")"
    def stringify = {
      val sb = new java.lang.StringBuilder(value.size + 4)
      var t: String = ""
      sb.append('"')
      value foreach {c =>
        c match {
          case '"' | '\\' => sb.append('\\'); sb.append(c)
          case '/'        => sb.append('\\'); sb.append(c)
          case '\b'       => sb.append("\\b")
          case '\f'       => sb.append("\\f")
          case '\n'       => sb.append("\\n")
          case '\r'       => sb.append("\\r")
          case '\t'       => sb.append("\\t")
          case _ if (c < 32  ||  c >= 127) =>
            val t = "000" + java.lang.Integer.toHexString(c)
            sb.append("\\u")
            sb.append(t.substring(t.size - 4))
          case _ =>
            sb.append(c)
        }
      }
      sb.append('"')
      sb.toString
    }
  }
  object JsonString {
    def parse(str: String): Option[JsonString] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonString] =
      if (!p.done  &&  p.get == '"') {
        p.save()
        val sb = new java.lang.StringBuilder
        p.update(1)
        while (!p.done  &&  p.get != '"') {
          if (p.get == '\\') {
            p.update(1)
            if (!p.done)
              p.get match {
                case '"'  => sb.append('"');  p.update(1)
                case '\\' => sb.append('\\'); p.update(1)
                case '/'  => sb.append('/');  p.update(1)
                case 'b'  => sb.append('\b'); p.update(1)
                case 'f'  => sb.append('\f'); p.update(1)
                case 'n'  => sb.append('\n'); p.update(1)
                case 'r'  => sb.append('\r'); p.update(1)
                case 't'  => sb.append('\t'); p.update(1)
                case 'u' if (p.remaining >= 5) =>
                  p.update(1)
                  sb.append(java.lang.Integer.parseInt(p.get(4)).toChar)
                  p.update(4)
                case _ =>
                  p.restore()
                  return None
              }
            else {
              p.restore()
              return None
            }
          }
          else {
            sb.append(p.get)
            p.update(1)
          }
        }
        if (p.get == '"') {
          p.update(1)
          p.unsave()
          Some(JsonString(sb.toString))
        }
        else {
          p.restore()
          None
        }
      }
      else
        None
  }

  trait JsonContainer extends Json
  object JsonContainer {
    def unapplySeq(x: Json): Option[Seq[_]] = x match {
      case JsonArray(elements @ _*) => Some(elements)
      case JsonObject(pairs @ _*) => Some(pairs)
      case _ => None
    }
    def parse(str: String): Option[JsonContainer] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonContainer] = JsonArray.parse(p) orElse JsonObject.parse(p)
  }

  case class JsonArray(elements: Json*) extends JsonContainer {
    override def toString() = "JsonArray(" + elements.mkString(", ") + ")"
    def stringify = "[" + elements.map(_.toString).mkString(", ") + "]"
    def all[T <: Json] = elements.forall(_.isInstance[T])
    def any[T <: Json] = elements.exists(_.isInstance[T])
    def to[T <: Json] = elements.map(_.asInstanceOf[T])
  }
  object JsonArray {
    def apply[V](elements: V*)(implicit fv: V => Json) = new JsonArray(elements.map(fv): _*)
    def parse(str: String): Option[JsonArray] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonArray] =
      if (!p.done  &&  p.get == '[') {
        p.update(1)
        whitespace(p)
        if (!p.done  &&  p.get == ']') {
          p.update(1)
          return Some(JsonArray())
        }
        p.save()
        val builder = List.newBuilder[Json]
        var done = false
        while (!done) {
          Json.parse(p) match {
            case Some(x) =>
              builder += x
            case None =>
              p.restore()
              return None
          }
          whitespace(p)
          if (!p.done  &&  p.get == ',') {
            p.update(1)
          }
          else {
            done = true
          }
          whitespace(p)
        }
        if (!p.done  &&  p.get == ']') {
          p.update(1)
          p.unsave()
          Some(JsonArray(builder.result: _*))
        }
        else {
          p.restore()
          return None
        }
      }
      else
        None
  }

  case class JsonObject(pairs: (JsonString, Json)*) extends JsonContainer {
    override def toString() = "JsonObject(" + pairs.map({case (k, v) => k.toString + " -> " + v.toString}).mkString(", ") + ")"
    def stringify = "{" + pairs.map({case (k, v) => k.toString + ": " + v.toString}).mkString(", ") + "}"
    def all[T <: Json] = pairs.forall(_._2.isInstance[T])
    def any[T <: Json] = pairs.exists(_._2.isInstance[T])
    def to[T <: Json] = pairs.map({case (k, v) => (k.value, v.asInstanceOf[T])})
  }
  object JsonObject {
    def apply[K, V](elements: (K, V)*)(implicit fk: K => JsonString, fv: V => Json) = new JsonObject(elements.map({case (k, v) => (fk(k), fv(v))}): _*)
    def parse(str: String): Option[JsonObject] = parseFully(str, parse(_))
    def parse(p: ParseState): Option[JsonObject] =
      if (!p.done  &&  p.get == '{') {
        p.update(1)
        whitespace(p)
        if (!p.done  &&  p.get == '}') {
          p.update(1)
          return Some(JsonObject())
        }
        p.save()
        val builder = List.newBuilder[(JsonString, Json)]
        var done = false
        while (!done) {
          val key = JsonString.parse(p) match {
            case Some(x) => x
            case None =>
              p.restore()
              return None
          }
          whitespace(p)
          if (!p.done  &&  p.get == ':')
            p.update(1)
          else {
            p.restore()
            return None
          }
          whitespace(p)
          Json.parse(p) match {
            case Some(x) =>
              builder += (key -> x)
            case None =>
              p.restore()
              return None
          }
          whitespace(p)
          if (!p.done  &&  p.get == ',')
            p.update(1)
          else
            done = true
          whitespace(p)
        }
        if (!p.done  &&  p.get == '}') {
          p.update(1)
          p.unsave()
          Some(JsonObject(builder.result: _*))
        }
        else {
          p.restore()
          return None
        }
      }
      else
        None
  }

  // Nice idea, but I have to unpack everything with pattern-matching anyway...

  // package check {
  //   sealed trait Validity {
  //     def apply(x: Json): Boolean
  //     def |(that: Validity) = Or(this, that)
  //     def &(that: Validity) = And(this, that)
  //   }

  //   case object Null extends Validity {
  //     def apply(x: Json) = x match {case JsonNull => true; case _ => false}
  //     override def toString() = "check.Null"
  //   }

  //   case object Primitive extends Validity {
  //     def apply(x: Json) = x match {case _: JsonPrimitive => true; case _ => false}
  //     override def toString() = "check.Primitive"
  //   }

  //   case object Boolean extends Validity {
  //     def apply(x: Json) = x match {case _: JsonBoolean => true; case _ => false}
  //     override def toString() = "check.Boolean"
  //   }

  //   case object Number extends Validity {
  //     def apply(x: Json) = x match {case _: JsonNumber => true; case _ => false}
  //     override def toString() = "check.Number"
  //   }

  //   case object Int extends Validity {
  //     def apply(x: Json) = x match {case _: JsonInt => true; case _ => false}
  //     override def toString() = "check.Int"
  //   }

  //   case object Float extends Validity {
  //     def apply(x: Json) = x match {case _: JsonFloat => true; case _ => false}
  //     override def toString() = "check.Float"
  //   }

  //   case object String extends Validity {
  //     def apply(x: Json) = x match {case _: JsonString => true; case _ => false}
  //     override def toString() = "check.String"
  //   }

  //   case class String(regex: scala.util.matching.Regex) extends Validity {
  //     def apply(x: Json) = x match {case JsonString(y) => !(regex unapplySeq y).isEmpty; case _ => false}
  //     override def toString() = s"check.String($regex)"
  //   }

  //   case object Container extends Validity {
  //     def apply(x: Json) = x match {case _: JsonContainer => true; case _ => false}
  //     override def toString() = "check.Container"
  //   }

  //   case class Container(items: Validity) extends Validity {
  //     def apply(x: Json) = x match {
  //       case JsonArray(elements @ _*) => elements.forall(items(_))
  //       case JsonObject(pairs @ _*) => pairs.map(_._2).forall(items(_))
  //       case _ => false
  //     }
  //     override def toString() = s"check.Container($items)"
  //   }

  //   case object Array extends Validity {
  //     def apply(x: Json) = x match {case _: JsonArray => true; case _ => false}
  //     override def toString() = "check.Array"
  //   }

  //   case class Array(items: Validity) extends Validity {
  //     def apply(x: Json) = x match {
  //       case JsonArray(elements @ _*) => elements.forall(items(_))
  //       case _ => false
  //     }
  //     override def toString() = s"check.Array($items)"
  //   }

  //   case object Object extends Validity {
  //     def apply(x: Json) = x match {case _: JsonObject => true; case _ => false}
  //     override def toString() = "check.Object"
  //   }

  //   case class Object(items: Validity) extends Validity {
  //     def apply(x: Json) = x match {
  //       case JsonObject(pairs @ _*) => pairs.map(_._2).forall(items(_))
  //       case _ => false
  //     }
  //     override def toString() = s"check.Object($items)"
  //   }

  //   case class Or(possibilities: Validity*) extends Validity {
  //     def apply(x: Json) = possibilities.exists(p => p(x))
  //     override def toString() = possibilities.mkString(" | ")
  //   }

  //   case class And(possibilities: Validity*) extends Validity {
  //     def apply(x: Json) = possibilities.forall(p => p(x))
  //     override def toString() = "(" + possibilities.mkString(" & ") + ")"
  //   }
  // }
}

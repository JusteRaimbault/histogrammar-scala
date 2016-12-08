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

package org.dianahep.histogrammar

//import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.sql.Column

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

//import org.apache.spark.sql.functions._

package sparksql {
  import org.dianahep.histogrammar.util.Compatible

  class HistogrammarAggregator[DATUM, CONTAINER <: Container[CONTAINER] with AggregationOnData {type Datum >: DATUM} : ClassTag](container: CONTAINER) extends Aggregator[DATUM, CONTAINER, CONTAINER] {

   def zero = container
   def reduce(h: CONTAINER, x: DATUM) = {h.fill(x); h}
   def merge(h1: CONTAINER, h2: CONTAINER) = h1 + h2
   def finish(whatever: CONTAINER): CONTAINER = whatever

   override def bufferEncoder: Encoder[CONTAINER] = Encoders.kryo[CONTAINER]
   override def outputEncoder: Encoder[CONTAINER] = Encoders.kryo[CONTAINER]

 }
}

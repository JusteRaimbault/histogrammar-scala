package test.scala.histogrammar

import scala.language.postfixOps

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.dianahep.histogrammar._
import org.dianahep.histogrammar.specialized.histogram._

class DefaultSuite extends FlatSpec with Matchers {
  val simple = List(3.4, 2.2, -1.8, 0.0, 7.3, -4.7, 1.6, 0.0, -3.0, -1.7)

  case class Struct(bool: Boolean, int: Int, double: Double, string: String)

  val struct = List(
    Struct(true,  -2,  3.4, "one"),
    Struct(false, -1,  2.2, "two"),
    Struct(true,   0, -1.8, "three"),
    Struct(false,  1,  0.0, "four"),
    Struct(false,  2,  7.3, "five"),
    Struct(false,  3, -4.7, "six"),
    Struct(true,   4,  1.6, "seven"),
    Struct(true,   5,  0.0, "eight"),
    Struct(false,  6, -3.0, "nine"),
    Struct(true,   7, -1.7, "ten"))

  val backward = struct.reverse

  // straightforward mean and variance to complement the Tony Finch calculations used in the module

  def mean(x: List[Double]) =
    if (x.isEmpty)
      0.0
    else
      x.sum / x.size

  def mean(x: List[Double], w: List[Double]) =
    if (w.filter(_ > 0.0).isEmpty)
      0.0
    else
      (x zip w map {case (xi, wi) => xi * Math.max(wi, 0.0)} sum) / w.filter(_ > 0.0).sum

  def variance(x: List[Double]) =
    if (x.isEmpty)
      0.0
    else
      x.map(Math.pow(_, 2)).sum / x.size - Math.pow(x.sum / x.size, 2)

  def variance(x: List[Double], w: List[Double]) =
    if (w.filter(_ > 0.0).isEmpty)
      0.0
    else
      (x zip w map {case (xi, wi) => xi * xi * Math.max(wi, 0.0)} sum) / w.filter(_ > 0.0).sum - Math.pow((x zip w map {case (xi, wi) => xi * Math.max(wi, 0.0)} sum) / w.filter(_ > 0.0).sum, 2)

  def mae(x: List[Double]) =
    if (x.isEmpty)
      0.0
    else
      x.map(Math.abs).sum / x.size

  def mae(x: List[Double], w: List[Double]) =
    if (w.filter(_ > 0.0).isEmpty)
      0.0
    else
      (x zip w map {case (xi, wi) => Math.abs(xi) * Math.max(wi, 0.0)} sum) / w.filter(_ > 0.0).sum

  // //////////////////////////////////////////////////////////////// Count/Counted/Counting

  // "Count/Counting/Counted" must "work unfiltered" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftCounting = Count()
  //     val rightCounting = Count()

  //     left.foreach(leftCounting.fill(_))
  //     right.foreach(rightCounting.fill(_))

  //     val (Count(leftResult), Count(rightResult)) = (leftCounting, rightCounting)

  //     leftResult should be (left.size)
  //     rightResult should be (right.size)

  //     val Count(finalResult) = leftCounting + rightCounting

  //     finalResult should be (simple.size)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Sum/Summed/Summing

  // "Sum/Summing/Summed" must "work unfiltered" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftSumming = Sum({x: Double => x})
  //     val rightSumming = Sum({x: Double => x})

  //     left.foreach(leftSumming.fill(_))
  //     right.foreach(rightSumming.fill(_))

  //     val (Sum(leftResult), Sum(rightResult)) = (leftSumming, rightSumming)

  //     leftResult should be (left.sum +- 1e-12)
  //     rightResult should be (right.sum +- 1e-12)

  //     val Sum(finalResult) = leftSumming + rightSumming

  //     finalResult should be (simple.sum +- 1e-12)
  //   }
  // }

  // it must "work with a filter" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftSumming = Sum({x: Struct => x.double}, {x: Struct => x.bool})
  //     val rightSumming = Sum({x: Struct => x.double}, {x: Struct => x.bool})

  //     left.foreach(leftSumming.fill(_))
  //     right.foreach(rightSumming.fill(_))

  //     val (Sum(leftResult), Sum(rightResult)) = (leftSumming, rightSumming)

  //     leftResult should be (left.filter(_.bool).map(_.double).sum +- 1e-12)
  //     rightResult should be (right.filter(_.bool).map(_.double).sum +- 1e-12)

  //     val Sum(finalResult) = leftSumming + rightSumming

  //     finalResult should be (struct.filter(_.bool).map(_.double).sum +- 1e-12)
  //   }
  // }

  // it must "work with a weighting factor" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftSumming = Sum({x: Struct => x.double}, {x: Struct => x.int})
  //     val rightSumming = Sum({x: Struct => x.double}, {x: Struct => x.int})

  //     left.foreach(leftSumming.fill(_))
  //     right.foreach(rightSumming.fill(_))

  //     val (Sum(leftResult), Sum(rightResult)) = (leftSumming, rightSumming)

  //     leftResult should be (left.filter(_.int >= 0).map({x => x.int * x.double}).sum +- 1e-12)
  //     rightResult should be (right.filter(_.int >= 0).map({x => x.int * x.double}).sum +- 1e-12)

  //     val Sum(finalResult) = leftSumming + rightSumming

  //     finalResult should be (struct.filter(_.int >= 0).map({x => x.int * x.double}).sum +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Average/Averaged/Averaging

  // "Average/Averaging/Averaged" must "work unfiltered" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftAveraging = Average({x: Double => x})
  //     val rightAveraging = Average({x: Double => x})

  //     left.foreach(leftAveraging.fill(_))
  //     right.foreach(rightAveraging.fill(_))

  //     val (Average(_, leftResult), Average(_, rightResult)) = (leftAveraging, rightAveraging)

  //     leftResult should be (mean(left) +- 1e-12)
  //     rightResult should be (mean(right) +- 1e-12)

  //     val Average(_, finalResult) = leftAveraging + rightAveraging

  //     finalResult should be (mean(simple) +- 1e-12)
  //   }
  // }

  // it must "work with a filter" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftAveraging = Average({x: Struct => x.double}, {x: Struct => x.bool})
  //     val rightAveraging = Average({x: Struct => x.double}, {x: Struct => x.bool})

  //     left.foreach(leftAveraging.fill(_))
  //     right.foreach(rightAveraging.fill(_))

  //     val (Average(_, leftResult), Average(_, rightResult)) = (leftAveraging, rightAveraging)

  //     leftResult should be (mean(left.filter(_.bool).map(_.double)) +- 1e-12)
  //     rightResult should be (mean(right.filter(_.bool).map(_.double)) +- 1e-12)

  //     val Average(_, finalResult) = leftAveraging + rightAveraging

  //     finalResult should be (mean(struct.filter(_.bool).map(_.double)) +- 1e-12)
  //   }
  // }

  // it must "work with a weighting factor" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftAveraging = Average({x: Struct => x.double}, {x: Struct => x.int})
  //     val rightAveraging = Average({x: Struct => x.double}, {x: Struct => x.int})

  //     left.foreach(leftAveraging.fill(_))
  //     right.foreach(rightAveraging.fill(_))

  //     val (Average(_, leftResult), Average(_, rightResult)) = (leftAveraging, rightAveraging)

  //     leftResult should be (mean(left.map(_.double), left.map(_.int.toDouble)) +- 1e-12)
  //     rightResult should be (mean(right.map(_.double), right.map(_.int.toDouble)) +- 1e-12)

  //     val Average(_, finalResult) = leftAveraging + rightAveraging

  //     finalResult should be (mean(struct.map(_.double), struct.map(_.int.toDouble)) +- 1e-12)
  //   }
  // }

  // it must "work in reverse" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = backward.splitAt(i)

  //     val leftAveraging = Average({x: Struct => x.double}, {x: Struct => x.int})
  //     val rightAveraging = Average({x: Struct => x.double}, {x: Struct => x.int})

  //     left.foreach(leftAveraging.fill(_))
  //     right.foreach(rightAveraging.fill(_))

  //     val (Average(_, leftResult), Average(_, rightResult)) = (leftAveraging, rightAveraging)

  //     leftResult should be (mean(left.map(_.double), left.map(_.int.toDouble)) +- 1e-12)
  //     rightResult should be (mean(right.map(_.double), right.map(_.int.toDouble)) +- 1e-12)

  //     val Average(_, finalResult) = leftAveraging + rightAveraging

  //     finalResult should be (mean(backward.map(_.double), backward.map(_.int.toDouble)) +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Deviate/Deviated/Deviating

  // "Deviate/Deviating/Deviated" must "work unfiltered" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftDeviating = Deviate({x: Double => x})
  //     val rightDeviating = Deviate({x: Double => x})

  //     left.foreach(leftDeviating.fill(_))
  //     right.foreach(rightDeviating.fill(_))

  //     val (Deviate(_, _, leftResult), Deviate(_, _, rightResult)) = (leftDeviating, rightDeviating)

  //     leftResult should be (variance(left) +- 1e-12)
  //     rightResult should be (variance(right) +- 1e-12)

  //     val Deviate(_, _, finalResult) = leftDeviating + rightDeviating

  //     finalResult should be (variance(simple) +- 1e-12)
  //   }
  // }

  // it must "work with a filter" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.bool})
  //     val rightDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.bool})

  //     left.foreach(leftDeviating.fill(_))
  //     right.foreach(rightDeviating.fill(_))

  //     val (Deviate(_, _, leftResult), Deviate(_, _, rightResult)) = (leftDeviating, rightDeviating)

  //     leftResult should be (variance(left.filter(_.bool).map(_.double)) +- 1e-12)
  //     rightResult should be (variance(right.filter(_.bool).map(_.double)) +- 1e-12)

  //     val Deviate(_, _, finalResult) = leftDeviating + rightDeviating

  //     finalResult should be (variance(struct.filter(_.bool).map(_.double)) +- 1e-12)
  //   }
  // }

  // it must "work with a weighting factor" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = struct.splitAt(i)

  //     val leftDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.int})
  //     val rightDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.int})

  //     left.foreach(leftDeviating.fill(_))
  //     right.foreach(rightDeviating.fill(_))

  //     val (Deviate(_, _, leftResult), Deviate(_, _, rightResult)) = (leftDeviating, rightDeviating)

  //     leftResult should be (variance(left.map(_.double), left.map(_.int.toDouble)) +- 1e-12)
  //     rightResult should be (variance(right.map(_.double), right.map(_.int.toDouble)) +- 1e-12)

  //     val Deviate(_, _, finalResult) = leftDeviating + rightDeviating

  //     finalResult should be (variance(struct.map(_.double), struct.map(_.int.toDouble)) +- 1e-12)
  //   }
  // }

  // it must "work in reverse" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = backward.splitAt(i)

  //     val leftDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.int})
  //     val rightDeviating = Deviate({x: Struct => x.double}, {x: Struct => x.int})

  //     left.foreach(leftDeviating.fill(_))
  //     right.foreach(rightDeviating.fill(_))

  //     val (Deviate(_, _, leftResult), Deviate(_, _, rightResult)) = (leftDeviating, rightDeviating)

  //     leftResult should be (variance(left.map(_.double), left.map(_.int.toDouble)) +- 1e-12)
  //     rightResult should be (variance(right.map(_.double), right.map(_.int.toDouble)) +- 1e-12)

  //     val Deviate(_, _, finalResult) = leftDeviating + rightDeviating

  //     finalResult should be (variance(backward.map(_.double), backward.map(_.int.toDouble)) +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// AbsoluteErr/AbsoluteErring/AbsoluteErred

  // "AbsoluteErr/AbsoluteErring/AbsoluteErred" must "work" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftAbsoluteErring = AbsoluteErr({x: Double => x})
  //     val rightAbsoluteErring = AbsoluteErr({x: Double => x})

  //     left.foreach(leftAbsoluteErring.fill(_))
  //     right.foreach(rightAbsoluteErring.fill(_))

  //     val (AbsoluteErr(_, leftResult), AbsoluteErr(_, rightResult)) = (leftAbsoluteErring, rightAbsoluteErring)

  //     leftResult should be (mae(left) +- 1e-12)
  //     rightResult should be (mae(right) +- 1e-12)

  //     val AbsoluteErr(_, finalResult) = leftAbsoluteErring + rightAbsoluteErring

  //     finalResult should be (mae(simple) +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Minimize/Minimizing/Minimized

  // "Minimize/Minimizing/Minimized" must "work" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftMinimizing = Minimize({x: Double => x})
  //     val rightMinimizing = Minimize({x: Double => x})

  //     left.foreach(leftMinimizing.fill(_))
  //     right.foreach(rightMinimizing.fill(_))

  //     val (Minimize(leftResult), Minimize(rightResult)) = (leftMinimizing, rightMinimizing)

  //     if (left.isEmpty) leftResult.isNaN should be (true)
  //     else leftResult should be (left.min +- 1e-12)
  //     if (right.isEmpty) rightResult.isNaN should be (true)
  //     else rightResult should be (right.min +- 1e-12)

  //     val Minimize(finalResult) = leftMinimizing + rightMinimizing

  //     if (simple.isEmpty) finalResult.isNaN should be (true)
  //     else finalResult should be (simple.min +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Maximize/Maximizing/Maximized

  // "Maximize/Maximizing/Maximized" must "work" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val leftMaximizing = Maximize({x: Double => x})
  //     val rightMaximizing = Maximize({x: Double => x})

  //     left.foreach(leftMaximizing.fill(_))
  //     right.foreach(rightMaximizing.fill(_))

  //     val (Maximize(leftResult), Maximize(rightResult)) = (leftMaximizing, rightMaximizing)

  //     if (left.isEmpty) leftResult.isNaN should be (true)
  //     else leftResult should be (left.max +- 1e-12)
  //     if (right.isEmpty) rightResult.isNaN should be (true)
  //     else rightResult should be (right.max +- 1e-12)

  //     val Maximize(finalResult) = leftMaximizing + rightMaximizing

  //     if (simple.isEmpty) finalResult.isNaN should be (true)
  //     else finalResult should be (simple.max +- 1e-12)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Bin/Binned/Binning

  // "Bin/Binning/Binned" must "work with Count/Counting/Counted" in {
  //   val one = Bin(5, -3.0, 7.0, {x: Double => x})
  //   simple.foreach(one.fill(_))
  //   one.values.map(_.value).toList should be (List(3.0, 2.0, 2.0, 1.0, 0.0))
  //   one.underflow.value should be (1.0)
  //   one.overflow.value should be (1.0)
  //   one.nanflow.value should be (0.0)

  //   val two = Bin(5, -3.0, 7.0, {x: Struct => x.double}, {x: Struct => x.bool})
  //   struct.foreach(two.fill(_))
  //   two.values.map(_.value).toList should be (List(2.0, 1.0, 1.0, 1.0, 0.0))
  //   two.underflow.value should be (0.0)
  //   two.overflow.value should be (0.0)
  //   two.nanflow.value should be (0.0)
  // }

  // "Binning/Binned" must "work with Sum/Summing/Summed" in {
  //   val one = Bin(5, -3.0, 7.0, {x: Double => x}, unweighted, Sum({x: Double => 10.0}), Sum({x: Double => 10.0}), Sum({x: Double => 10.0}), Sum({x: Double => 10.0}))
  //   simple.foreach(one.fill(_))
  //   one.values.map(_.value).toList should be (List(30.0, 20.0, 20.0, 10.0, 0.0))
  //   one.underflow.value should be (10.0)
  //   one.overflow.value should be (10.0)
  //   one.nanflow.value should be (0.0)

  //   val two = Bin(5, -3.0, 7.0, {x: Struct => x.double}, {x: Struct => x.bool}, Sum({x: Struct => 10.0}), Sum({x: Struct => 10.0}), Sum({x: Struct => 10.0}), Sum({x: Struct => 10.0}))
  //   struct.foreach(two.fill(_))
  //   two.values.map(_.value).toList should be (List(20.0, 10.0, 10.0, 10.0, 0.0))
  //   two.underflow.value should be (0.0)
  //   two.overflow.value should be (0.0)
  //   two.nanflow.value should be (0.0)
  // }

  // //////////////////////////////////////////////////////////////// SparselyBin/SparselyBinned/SparselyBinning

  // "SparselyBin/SparselyBinned/SparselyBinning" must "work with Count/Counting/Counted" in {
  //   val one = SparselyBin(1.0, {x: Double => x})
  //   simple.foreach(one.fill(_))
  //   Factory.fromJson(one.toJson).as[SparselyBinned[Counted, Counted]].values.map({case (k, v) => (k, v.value)}).toList should be (List(-5 -> 1.0, -3 -> 1.0, -2 -> 2.0, 0 -> 2.0, 1 -> 1.0, 2 -> 1.0, 3 -> 1.0, 7 -> 1.0))

  //   one.numFilled should be (8)
  //   one.num should be (12)
  //   one.low should be (-5.0)
  //   one.high should be (8.0)
  // }

  // //////////////////////////////////////////////////////////////// Fraction/Fractioned/Fractioning

  // "Fraction/Fractioned/Fractioning" must "work with Count/Counting/Counted" in {
  //   val fracking = Fraction({x: Double => x > 0.0}, Count())
  //   simple.foreach(fracking.fill(_))

  //   fracking.numerator.value should be (4.0)
  //   fracking.denominator.value should be (10.0)
  // }

  // it must "work with Sum/Summing/Summed" in {
  //   val fracking = Fraction({x: Double => x > 0.0}, Sum({x: Double => x}))
  //   simple.foreach(fracking.fill(_))

  //   fracking.numerator.value should be (14.5 +- 1e-12)
  //   fracking.denominator.value should be (3.3 +- 1e-12)
  // }

  // it must "work with Histogram/Histogramming/Histogrammed" in {
  //   val fracking = Fraction({x: Double => x > 0.0}, Histogram(5, -3.0, 7.0, {x: Double => x}))
  //   simple.foreach(fracking.fill(_))

  //   fracking.numerator.values.map(_.value).toList should be (List(0.0, 0.0, 2.0, 1.0, 0.0))
  //   fracking.denominator.values.map(_.value).toList should be (List(3.0, 2.0, 2.0, 1.0, 0.0))

  //   fracking match {
  //     case Fraction(
  //       Bin(Seq(Count(0.0), Count(0.0), Count(2.0), Count(1.0), Count(0.0)), _, _, _),
  //       Bin(Seq(Count(3.0), Count(2.0), Count(2.0), Count(1.0), Count(0.0)), _, _, _)) => 1 should be (1)
  //     case _ => 0 should be (1)
  //   }
  // }

  // //////////////////////////////////////////////////////////////// Stack/Stacked/Stacking

  // "Stack/Stacked/Stacking" must "work with Count/Counting/Counted" in {
  //   val stacking = Stack(Count(), {x: Double => x}, 0.0, 2.0, 4.0, 6.0, 8.0)
  //   simple.foreach(stacking.fill(_))

  //   stacking.cuts.map({case (k, v) => (k, v.value)}).toList should be (List(java.lang.Double.NEGATIVE_INFINITY -> 10.0, 0.0 -> 6.0, 2.0 -> 3.0, 4.0 -> 1.0, 6.0 -> 1.0, 8.0 -> 0.0))
  // }

  // it must "work with Sum/Summing/Summed" in {
  //   val stacking = Stack(Sum({x: Double => x}), {x: Double => x}, 0.0, 2.0, 4.0, 6.0, 8.0)
  //   simple.foreach(stacking.fill(_))

  //   stacking.cuts(1)._2.value should be (14.5 +- 1e-12)
  // }

  // //////////////////////////////////////////////////////////////// Partition/Partitioned/Partitioning

  // "Partition/Partitioned/Partitioning" must "work with Count/Counting/Counted" in {
  //   val partitioning = Partition(Count(), {x: Double => x}, 0.0, 2.0, 4.0, 6.0, 8.0)
  //   simple.foreach(partitioning.fill(_))
    
  //   partitioning.cuts.map({case (k, v) => (k, v.value)}).toList should be (List(java.lang.Double.NEGATIVE_INFINITY -> 4.0, 0.0 -> 3.0, 2.0 -> 2.0, 4.0 -> 0.0, 6.0 -> 1.0, 8.0 -> 0.0))
  // }

  // it must "work with Sum/Summing/Summed" in {
  //   val partitioning = Partition(Sum({x: Double => x}), {x: Double => x}, 0.0, 2.0, 4.0, 6.0, 8.0)
  //   simple.foreach(partitioning.fill(_))

  //   partitioning.cuts(0)._2.value should be (-11.2 +- 1e-12)
  //   partitioning.cuts(1)._2.value should be (1.6 +- 1e-12)
  // }

  // //////////////////////////////////////////////////////////////// Categorize/Categorized/Categorizing

  // "Categorize/Categorized/Categorizing" must "work" in {
  //   val categorizing = Categorize({x: Struct => x.string.substring(0, 1)})
  //   struct.foreach(categorizing.fill(_))

  //   categorizing.pairsMap map {case (k, v) => (k, v.value)} should be (Map("n" -> 1.0, "e" -> 1.0, "t" -> 3.0, "s" -> 2.0, "f" -> 2.0, "o" -> 1.0))
  // }

  // //////////////////////////////////////////////////////////////// Label/Labeled/Labeling

  // "Label/Labeled/Labeling" must "work with a single type" in {
  //   val one = Histogram(5, -3.0, 7.0, {x: Double => x})
  //   val two = Histogram(10, 0.0, 10.0, {x: Double => x})
  //   val three = Histogram(5, -3.0, 7.0, {x: Double => 2*x})

  //   val labeling = Label("one" -> one, "two" -> two, "three" -> three)

  //   simple.foreach(labeling.fill(_))

  //   labeling("one").numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //   labeling("two").numericalValues should be (Seq(2.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0))
  //   labeling("three").numericalValues should be (Seq(0.0, 2.0, 0.0, 2.0, 1.0))
  // }

  // it must "permit histograms to have different cuts" in {
  //   val one = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 0})
  //   val two = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 5})
  //   val three = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x < 5})

  //   val labeling = Label("one" -> one, "two" -> two, "three" -> three)

  //   simple.foreach(labeling.fill(_))

  //   labeling("one").numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 0.0, 1.0, 0.0))
  //   labeling("two").numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0))
  //   labeling("three").numericalValues should be (Seq(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 2.0, 0.0, 0.0, 0.0))
  // }

  // //////////////////////////////////////////////////////////////// UntypedLabel/UntypedLabeled/UntypedLabeling

  // "UntypedLabel/UntypedLabeled/UntypedLabeling" must "work with a single type" in {
  //   val one = Histogram(5, -3.0, 7.0, {x: Double => x})
  //   val two = Histogram(10, 0.0, 10.0, {x: Double => x})
  //   val three = Histogram(5, -3.0, 7.0, {x: Double => 2*x})

  //   val labeling = UntypedLabel("one" -> one, "two" -> two, "three" -> three)

  //   simple.foreach(labeling.fill(_))

  //   labeling("one").as[one.Type].numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //   labeling("two").as[two.Type].numericalValues should be (Seq(2.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0))
  //   labeling("three").as[three.Type].numericalValues should be (Seq(0.0, 2.0, 0.0, 2.0, 1.0))
  // }

  // it must "permit histograms to have different cuts" in {
  //   val one = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 0})
  //   val two = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 5})
  //   val three = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x < 5})

  //   val labeling = UntypedLabel("one" -> one, "two" -> two, "three" -> three)

  //   simple.foreach(labeling.fill(_))

  //   labeling("one").as[one.Type].numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 0.0, 1.0, 0.0))
  //   labeling("two").as[two.Type].numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0))
  //   labeling("three").as[three.Type].numericalValues should be (Seq(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 2.0, 0.0, 0.0, 0.0))
  // }

  // it must "work with multiple types" in {
  //   val one = Histogram(5, -3.0, 7.0, {x: Double => x})
  //   val two = Sum({x: Double => 1.0})
  //   val three = Deviate({x: Double => x + 100.0})

  //   val mapping = UntypedLabel("one" -> one, "two" -> two, "three" -> three)

  //   simple.foreach(mapping.fill(_))

  //   mapping("one").as[one.Type].values.map(_.value).toList should be (List(3.0, 2.0, 2.0, 1.0, 0.0))
  //   mapping("two").as[two.Type].value should be (10.0)
  //   mapping("three").as[three.Type].totalWeight should be (10.0 +- 1e-12)
  //   mapping("three").as[three.Type].mean should be (100.33 +- 1e-12)
  //   mapping("three").as[three.Type].variance should be (10.8381 +- 1e-12)   // just to be different
  // }

  // //////////////////////////////////////////////////////////////// Index/Indexed/Indexing

  // "Index/Indexed/Indexing" must "work with a single type" in {
  //   val one = Histogram(5, -3.0, 7.0, {x: Double => x})
  //   val two = Histogram(10, 0.0, 10.0, {x: Double => x})
  //   val three = Histogram(5, -3.0, 7.0, {x: Double => 2*x})

  //   val indexing = Index(one, two, three)

  //   simple.foreach(indexing.fill(_))

  //   indexing(0).numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //   indexing(1).numericalValues should be (Seq(2.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0))
  //   indexing(2).numericalValues should be (Seq(0.0, 2.0, 0.0, 2.0, 1.0))
  // }

  // it must "permit histograms to have different cuts" in {
  //   val one = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 0})
  //   val two = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x > 5})
  //   val three = Histogram(10, -10, 10, {x: Double => x}, {x: Double => x < 5})

  //   val indexing = Index(one, two, three)

  //   simple.foreach(indexing.fill(_))

  //   indexing(0).numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 0.0, 1.0, 0.0))
  //   indexing(1).numericalValues should be (Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0))
  //   indexing(2).numericalValues should be (Seq(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 2.0, 0.0, 0.0, 0.0))
  // }

  // //////////////////////////////////////////////////////////////// Branch/Branched/Branching

  // "Branch/Branched/Branching" must "permit multiple types without losing type information" in {
  //   val one = Histogram(5, -3.0, 7.0, {x: Double => x})
  //   val two = Count()
  //   val three = Deviate({x: Double => x + 100.0})

  //   val branching = Branch(one, two, three)

  //   simple.foreach(branching.fill(_))

  //   branching.i0.values.map(_.value).toList should be (List(3.0, 2.0, 2.0, 1.0, 0.0))
  //   branching.i0.underflow.value should be (1.0)
  //   branching.i0.overflow.value should be (1.0)
  //   branching.i0.nanflow.value should be (0.0)

  //   branching.i1.value should be (10L)

  //   branching.i2.totalWeight should be (10.0 +- 1e-12)
  //   branching.i2.mean should be (100.33 +- 1e-12)
  //   branching.i2.variance should be (10.8381 +- 1e-12)
  // }

  // //////////////////////////////////////////////////////////////// Usability in fold/aggregate

  // "Aggregators/Combiners" must "be usable in Spark's aggregate (which has the same signature as Scala's foldLeft + reduce)" in {
  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val partialHists = Seq(
  //       left.foldLeft(Bin(5, -3.0, 7.0, {x: Double => x}))({(hist, x) => hist.fill(x); hist}),
  //       right.foldLeft(Bin(5, -3.0, 7.0, {x: Double => x}))({(hist, x) => hist.fill(x); hist}))

  //     val finalHist = partialHists.reduce(_ + _)

  //     finalHist.numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //     finalHist.numericalUnderflow should be (1.0)
  //     finalHist.numericalOverflow should be (1.0)
  //     finalHist.numericalNanflow should be (0.0)
  //   }

  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val hist1 = Bin(5, -3.0, 7.0, {x: Double => x})
  //     val hist2 = Bin(5, -3.0, 7.0, {x: Double => x})

  //     val partialHists = Seq(
  //       left.foldLeft(hist1)(increment[hist1.Type]),
  //       right.foldLeft(hist2)(increment[hist1.Type]))

  //     val finalHist = partialHists.reduce(combine[hist1.Type])

  //     finalHist.numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //     finalHist.numericalUnderflow should be (1.0)
  //     finalHist.numericalOverflow should be (1.0)
  //     finalHist.numericalNanflow should be (0.0)
  //   }

  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val hist1 = Bin(5, -3.0, 7.0, {x: Double => x})
  //     val hist2 = Bin(5, -3.0, 7.0, {x: Double => x})

  //     val collection1 = Label("hist" -> hist1)
  //     val collection2 = Label("hist" -> hist2)

  //     val partialHists = Seq(
  //       left.foldLeft(collection1)(increment[collection1.Type]),
  //       right.foldLeft(collection2)(increment[collection1.Type]))

  //     val finalHist = partialHists.reduce(combine[collection1.Type])

  //     finalHist("hist").numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //     finalHist("hist").numericalUnderflow should be (1.0)
  //     finalHist("hist").numericalOverflow should be (1.0)
  //     finalHist("hist").numericalNanflow should be (0.0)
  //   }

  //   for (i <- 0 to 10) {
  //     val (left, right) = simple.splitAt(i)

  //     val hist1 = Bin(5, -3.0, 7.0, {x: Double => x})
  //     val hist2 = Bin(5, -3.0, 7.0, {x: Double => x})
  //     val sum1 = Sum({x: Double => 1.0})
  //     val sum2 = Sum({x: Double => 1.0})

  //     val collection1 = UntypedLabel("hist" -> hist1, "sum" -> sum1)
  //     val collection2 = UntypedLabel("hist" -> hist2, "sum" -> sum2)

  //     val partialHists = Seq(
  //       left.foldLeft(collection1)(incrementUntypedLabel[Double]),
  //       right.foldLeft(collection2)(incrementUntypedLabel[Double]))

  //     val finalHist = partialHists.reduce(combineUntypedLabel[Double])

  //     finalHist("hist").as[hist1.Type].numericalValues should be (Seq(3.0, 2.0, 2.0, 1.0, 0.0))
  //     finalHist("hist").as[hist1.Type].numericalUnderflow should be (1.0)
  //     finalHist("hist").as[hist1.Type].numericalOverflow should be (1.0)
  //     finalHist("hist").as[hist1.Type].numericalNanflow should be (0.0)
  //     finalHist("sum").as[sum1.Type].value should be (10.0)
  //   }
  // }
}

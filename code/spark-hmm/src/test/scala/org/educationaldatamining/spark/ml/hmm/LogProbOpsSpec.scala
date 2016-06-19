package org.educationaldatamining.spark.ml.hmm

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

import org.scalatest.{FlatSpec, Matchers}

/**
	* Test the operations of the LogProb class
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/16/16.
	*/
class LogProbOpsSpec extends FlatSpec with Matchers
{
	val verySmall: Double = 1E-15

	// create a bunch of values
	val zero = new LogProb(0.0)
	val q = new LogProb(0.25)
	val t = new LogProb( (1.0/3.0) )
	val half = new LogProb( 0.5 )
	val twot = new LogProb( (2.0/3.0) )
	val threeq = new LogProb( 0.75 )
	val one = new LogProb( 1.0 )

	/* Addition */

	// add to zero
	"0 + 0" should behave like correctAddition( zero, zero, zero )
	"0 + 1/4" should behave like correctAddition( zero, q, q )
	"0 + 1/3" should behave like correctAddition( zero, t, t )
	"1 + 0" should behave like correctAddition( one, zero, one )

	// sum to one
	"1/3 + 2/3" should behave like correctAddition( t, twot, one )
	"3/4 + 1/4" should behave like correctAddition( threeq, q, one )
	"1/2 + 1/2" should behave like correctAddition( half, half, one )

	// other sums
	"1/4 + 1/4" should behave like correctAddition( q, q, half )
	"1/3 + 1/3" should behave like correctAddition( t, t, twot )
	"1/4 + 1/2" should behave like correctAddition( q, half, threeq )

	/* Subtraction */

	// subtract zero
	"0 - 0" should behave like correctOp( () => zero - zero, zero )
	"1/4 - 0" should behave like correctOp( () => q - zero, q )
	"1/3 - 0" should behave like correctOp( () => t - zero, t )
	"1/2 - 0" should behave like correctOp( () => half - zero, half )
	"2/3 - 0" should behave like correctOp( () => twot - zero, twot )
	"3/4 - 0" should behave like correctOp( () => threeq - zero, threeq )
	"1 - 0" should behave like correctOp( () => one - zero, one )

	// subtract from one
	"1 - 3/4" should behave like correctOp( () => one - threeq, q )
	"1 - 2/3" should behave like correctOp( () => one - twot, t )
	"1 - 1/2" should behave like correctOp( () => one - half, half )
	"1 - 1/3" should behave like correctOp( () => one - t, twot )
	"1 - 1/4" should behave like correctOp( () => one - q, threeq )
	"1 - 1" should behave like correctOp( () => one - one, zero )

	// subtract to zero
	"1/3 - 1/3" should behave like correctOp( () => t - t, zero )
	"1/2 - 1/2" should behave like correctOp( () => half - half, zero )
	"3/4 - 3/4" should behave like correctOp( () => threeq - threeq, zero )

	// other even differences
	"1/2 - 1/4" should behave like correctOp( () => half - q, q )
	"3/4 - 1/4" should behave like correctOp( () => threeq - q, half )
	"2/3 - 1/3" should behave like correctOp( () => twot - t, t )
	"3/4 - 1/2" should behave like correctOp( () => threeq - half, q )

	// more uneven differences
	"1/3 - 1/4" should behave like correctOp( () => t - q, new LogProb( (1.0/3.0) - 0.25 ) )
	"1/2 - 1/3" should behave like correctOp( () => half - t, new LogProb( 0.5 - (1.0/3.0) ) )
	"2/3 - 1/2" should behave like correctOp( () => twot - half, new LogProb( (2.0/3.0) - 0.5 ) )
	"3/4 - 1/3" should behave like correctOp( () => threeq - t, new LogProb( 0.75 - (1.0/3.0) ) )
	"3/4 - 2/3" should behave like correctOp( () => threeq - twot, new LogProb( 0.75 - (2.0/3.0) ) )

	/* Multiplication */

	// multiply to zero
	"1 * 0" should behave like correctMultiplication( one, zero, zero )
	"1/4 * 0" should behave like correctMultiplication( q, zero, zero )
	"1/3 * 0" should behave like correctMultiplication( t, zero, zero )
	"1/2 * 0" should behave like correctMultiplication( half, zero, zero )

	// take a fraction from one
	"1 * 1/4" should behave like correctMultiplication( one, q, q )
	"1 * 1/3" should behave like correctMultiplication( one, t, t )
	"1 * 1/2" should behave like correctMultiplication( one, half, half )
	"1 * 2/3" should behave like correctMultiplication( one, twot, twot )
	"1 * 3/4" should behave like correctMultiplication( one, threeq, threeq )
	"1 * 1" should behave like correctMultiplication( one, one, one )

	// patterned products
	"1/2 * 2/3" should behave like correctMultiplication( half, twot, t )
	"1/2 * 1/2" should behave like correctMultiplication( half, half, q )
	"1/3 * 3/4" should behave like correctMultiplication( t, threeq, q )

	// some other products
	"1/4 * 1/4" should behave like correctMultiplication( q, q, new LogProb( (1.0/16.0) ) )
	"1/4 * 1/3" should behave like correctMultiplication( q, t, new LogProb( (1.0/12.0) ) )
	"1/4 * 2/3" should behave like correctMultiplication( q, twot, new LogProb( (1.0/6.0) ) )
	"1/4 * 3/4" should behave like correctMultiplication( q, threeq, new LogProb( (3.0/16.0) ) )
	"1/3 * 1/3" should behave like correctMultiplication( t, t, new LogProb( (1.0/9.0) ) )
	"1/2 * 1/3" should behave like correctMultiplication( half, t, new LogProb( (1.0/6.0) ) )
	"1/2 * 3/4" should behave like correctMultiplication( half, threeq, new LogProb( (3.0/8.0) ) )

	/* Division */

	// zero divided by anything (other than zero) is zero
	"0 / 1" should behave like correctOp( () => zero / one, zero )
	"0 / 1/3" should behave like correctOp( () => zero / t, zero )
	"0 / 1/2" should behave like correctOp( () => zero / half, zero )
	"0 / 3/4" should behave like correctOp( () => zero / threeq, zero )

	// anything divided by one is unchanged
	"1/4 / 1" should behave like correctOp( () => q / one, q )
	"1/3 / 1" should behave like correctOp( () => t / one, t )
	"1/2 / 1" should behave like correctOp( () => half / one, half )
	"2/3 / 1" should behave like correctOp( () => twot / one, twot )

	// anything (other than zero) divided by itself is one
	"1/4 / 1/4" should behave like correctOp( () => q / q, one )
	"1/3 / 1/3" should behave like correctOp( () => t / t, one )
	"1/2 / 1/2" should behave like correctOp( () => half / half, one )
	"1 / 1" should behave like correctOp( () => one / one, one )

	// patterned divisions
	"1/4 / 3/4" should behave like correctOp( () => q / threeq, t )
	"1/3 / 2/3" should behave like correctOp( () => t / twot, half )
	"1/2 / 2/3" should behave like correctOp( () => half / twot, threeq )
	"1/4 / 1/2" should behave like correctOp( () => q / half, half )
	"1/3 / 1/2" should behave like correctOp( () => t / half, twot )
	"1/2 / 3/4" should behave like correctOp( () => half / threeq, twot )

	/**
		*
		* @param x
		* @param y
		* @param correctResult
		*/
	def correctAddition( x: LogProb, y: LogProb, correctResult: LogProb ) =
		commutativeOp( x, y, (a: LogProb, b: LogProb) => a + b, correctResult )

	/**
		*
		* @param x
		* @param y
		* @param correctResult
		*/
	def correctMultiplication( x: LogProb, y: LogProb, correctResult: LogProb ) =
		commutativeOp( x, y, (a: LogProb, b: LogProb) => a * b, correctResult )

	/**
		* Test a commutative operation
		*
		* @param x
		* @param y
		* @param op
		* @param correctResult
		*/
	def commutativeOp( x: LogProb, y: LogProb,
	                   op: (LogProb,LogProb) => LogProb,
	                   correctResult: LogProb ): Unit =
	{
		// make sure it's correct
		correctOp( () => op(x,y), correctResult )

		it should "obey commutativity" in {
			op(x,y).value should be === (op(y,x).value)
			op(x,y).asProb should be === (op(y,x).asProb)
		}
	}

	/**
		* Test that an operation produces the correct result
		*
		* @param op the operation to test
		* @param correctResult the correct value
		*/
	def correctOp(op: () => LogProb, correctResult: LogProb ): Unit =
	{
		it should "produce a valid LogProb instance" in {
			noException should be thrownBy op()
			val result = op()
			result should not be null
			result shouldBe a [LogProb]
		}

		it should "produce the correct logProbability value" in {
			op().value should ===( correctResult.value +- verySmall )
		}

		it should "produce the correct probability value" in {
			val result = op()
			result.asProb should ===( correctResult.asProb +- verySmall )
			result.asProb should be >= 0.0
			result.asProb should be <= 1.0
		}
	}
}

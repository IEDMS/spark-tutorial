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
	* Test the LogProb class for different (in)valid values
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/16/16.
	*/
class LogProbValuesSpec extends FlatSpec with Matchers
{
	// Test some invalid probability values
	val invalidProbs = Array( ("-1", -1.0),
	                          ("42", 42.0),
	                          ("Just less than 0", 0.0 - Double.MinPositiveValue ),
	                          ("Just more than 1", 1.0 + 1E-15 ),
	                          ("Neg smallest value", - Double.MinPositiveValue ),
	                          ("PI", Math.PI),
	                          ("E", Math.E),
	                          ("Inf.", Double.PositiveInfinity),
	                          ("-Inf.", Double.NegativeInfinity),
	                          ("Min. Double", Double.MinValue),
	                          ("Max. Double", Double.MaxValue)
	                        )
	invalidProbs.foreach( np => testInvalidProb( np._1, np._2 ) )

	/* Set up some valid probability values */

	"Zero" should behave like validProb( 0.0, Double.NegativeInfinity )
	"1/4" should behave like validProb( 0.25 )
	"1/3" should behave like validProb( (1.0/3.0) )
	"Half" should behave like validProb( 0.5 )
	"2/3" should behave like validProb( (2.0/3.0) )
	"3/4" should behave like validProb( 0.75 )
	"One" should behave like validProb( 1.0, 0.0 )

	// Some valid values, but which require special handling:

	// Any NaN value will have NaN for both the probability and log-probability
	behavior of "NaN"
	def nanLP = newLogProb(Double.NaN)

	it should behave like instantiableLogProb( nanLP )

	it should "have a value of NaN" in {
		assert( nanLP.value.isNaN )
	}

	it should "have a probability of NaN" in {
		assert( nanLP.asProb.isNaN )
	}

	"Negative Zero" should behave like validProb( -0.0, Double.NegativeInfinity )

	it should "be equal to 0.0" in {
		val negZero = new LogProb(-0.0)
		negZero.asProb should be === 0.0
	}

	/**
		* Test Functions:
		*/

	/**
		* Curried constructor for the LogProb class
		* @param prob the probability value to use
		* @return a function which will return a LogProb instance of the given probability
		*/
	def newLogProb( prob: Double )() = new LogProb(prob)

	/**
		* Test an invalid probability value
		* @param name the name of the value to test
		* @param prob the probability value to test
		*/
	def testInvalidProb( name: String, prob: Double ): Unit =
	{
		name should "throw an IllegalArgumentException when creating a LogProb instance" in {
			an [IllegalArgumentException] should be thrownBy new LogProb(prob)
		}
	}

	/**
		* Tests that the provided value will instantiate to a valid LogProb instance
		* @param newLP a function returning a new LogProb instance
		*/
	def instantiableLogProb( newLP: => LogProb ): Unit =
	{
		it should "instantiate a LogProb instance" in {
			noException should be thrownBy newLP
			newLP should not be null
			newLP shouldBe a [LogProb]
		}
	}

	/**
		* Run tests on a valid probability value
		*
		* @param prob probability value to test
		* @param logProb the log-probability value it should take on
		*/
	def validProb( prob: Double, logProb: Double ): Unit =
	{
		def newLP = newLogProb(prob)

		instantiableLogProb( newLP )

		it should "have a value of "+ logProb in {
			newLP.value should === (logProb)
		}

		it should "have a probability value of "+ prob in {
			val lp = newLP
			lp.asProb should === (prob)
			lp.asProb should be >= 0.0
			lp.asProb should be <= 1.0
		}
	}

	/**
		* Helper that gives the log value
		*
		* @param prob probability value to test
		*/
	def validProb( prob: Double ): Unit = validProb( prob, Math.log(prob) )
}

package org.educationaldatamining.spark.ml.hmm

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

/**
	* A helper class to handle probability operations in log space
	* to avoid underflows and improve precision of calculations.
	* <p/>
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/15/16.
	*/
class LogProb private()
{
	// the log-transformed value
	var value: Double = Double.NaN

	/**
		* Constructor
		*
		* @param prob probability value
		*/
	def this( prob: Double ) =
	{
		this()
		value = logTransform(prob)
	}

	/**
		* Transform this log-probability back to a probability value in [0.0, 1.0]
		*
		* @return the probability value of this log-probability
		*/
	def asProb: Double =
	{
		if( value == Double.NegativeInfinity )
			0.0
		Math.exp(value)
	}

	/**
		* Log-transforms a probability value
		*
		* @param prob the probability value
		* @return the log-transformed probability value
		*/
	private def logTransform( prob: Double ): Double =
	{
		if( prob < 0.0 || prob > 1.0 )
			throw new IllegalArgumentException(prob +" is an invalid probabilty value. Probability values must be in [0.0, 1.0]")
		Math.log(prob)
	}


	/**
		* Perform a multiplication of probabilities in log-prob space
		*
		* @param other the other log-probability
		* @return a log-probability of the product
		*/
	def *( other: LogProb ): LogProb =
		LogProb.createAsLogOpResult( this.value, other.value,
		                             (x,y) => Double.NegativeInfinity,
		                             (x,y) => x + y )

	/**
		* Perform a division of probabilities in log-prob space
		*
		* @param divisor the other log-probability
		* @return a log-probability of the division
		*/
	def /( divisor: LogProb ): LogProb =
		LogProb.createAsLogOpResult( this.value, divisor.value,
		                             (x,y) => {
			                             if( y == Double.NegativeInfinity )
				                             Double.NaN
			                             else
				                             Double.NegativeInfinity
		                             },
		                             (x,y) => x - y )

	/**
		*
		* @param a
		* @param b
		* @return
		*/
	private def addOp( a: Double, b: Double ): Double =
	{
		if( a > b )
			a + Math.log1p( Math.exp( b - a ) )
		else
			b + Math.log1p( Math.exp( a - b ) )
	}

	/**
		* Perform a sum of probabilities in log-prob space
		*
		* @param other the other log-probability
		* @return a log-probability of the sum
		*/
	def +( other: LogProb ): LogProb =
		LogProb.createAsLogOpResult( this.value, other.value,
		                             (x,y) => if( x == Double.NegativeInfinity ) y else x,
		                             addOp )

	/**
		* Perform a difference of probabilities in log-prob space
		*
		* @param other the other log-probability
		* @return a log-probability of the difference
		*/
	def -( other: LogProb ) =
		LogProb.createAsLogOpResult( this.value, other.value,
		                             (x,y) => if( y == Double.NegativeInfinity ) x else -y,
		                             (x,y) => x + Math.log(1.0 - Math.exp(y -x)) )
}

/**
	* Static helper methods
	*/
object LogProb
{
	/** Some static values **/
	val ZERO = new LogProb(0.0)
	val QUARTER = new LogProb(0.25)
	val HALF = new LogProb(0.5)
	val THREE_QUARTERS = new LogProb(0.75)
	val ONE = new LogProb(1.0)

	/** methods **/

	/**
		* An internal create method that can specify the log-probability
		* @param value
		* @return
		*/
	private def create( value: Double ): LogProb =
	{
		val lp = new LogProb()
		lp.value = value
		lp
	}

	/**
		* Preform a log-probability operation
		*
		* @param a
		* @param b
		* @param zeroHandler
		* @param op
		* @return
		*/
	private def doLogOp( a: Double,
	                     b: Double,
	                     zeroHandler: (Double,Double) => Double,
	                     op: (Double,Double) => Double ): Double =
	{
		if( a == Double.NaN || b == Double.NaN )
			Double.NaN
		else  if( a == Double.NegativeInfinity || b == Double.NegativeInfinity )
			zeroHandler( a, b )
		else
			op( a, b )
	}

	/**
		*
		* @param a
		* @param b
		* @param zeroHandler
		* @param op
		* @return
		*/
	private def createAsLogOpResult( a: Double,
	                                 b: Double,
	                                 zeroHandler: (Double,Double) => Double,
	                                 op: (Double,Double) => Double ): LogProb =
		create( doLogOp( a, b, zeroHandler, op ) )
}

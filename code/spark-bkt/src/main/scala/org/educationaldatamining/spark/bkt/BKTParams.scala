package org.educationaldatamining.spark.bkt

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

import org.apache.spark.ml.param._

/**
	* Parameters for Bayesian Knowledge Tracing
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/18/16.
	*/
trait BKTParams extends Params
{
	/** BKT parameters **/

	/**
		* P-Init parameter
		*
		* @group param
		*/
	final val pInit: DoubleParam = new DoubleParam( this, "pInit",
	                                                "Probability of initial student knowledge",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Learn parameter
		*
		* @group param
		*/
	final val pLearn: DoubleParam = new DoubleParam( this, "pLearn",
	                                                 "Probability of learning",
	                                                 ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Guess parameter
		*
		* @group param
		*/
	final val pGuess: DoubleParam = new DoubleParam( this, "pGuess",
	                                                "Probability of guessing",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Slip parameter
		*
		* @group param
		*/
	final val pSlip: DoubleParam = new DoubleParam( this, "pSlip",
	                                                "Probability of making a mistake (slipping)",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/** Getters for the parameters **/

	/** @group getParam **/
	final def getPInit = $(pInit)

	/** @group getParam **/
	final def getPLearn = $(pLearn)

	/** @group getParam **/
	final def getPGuess = $(pGuess)

	/** @group getParam **/
	final def getPSlip = $(pSlip)
}

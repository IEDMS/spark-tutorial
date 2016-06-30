package org.educationaldatamining.spark.bkt

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* Licensed under the Apache License, Version 2.0 (the "License");
	* you may not use this file except in compliance with the License.
	* You may obtain a copy of the License at
	*
	* http://www.apache.org/licenses/LICENSE-2.0
	*
	* Unless required by applicable law or agreed to in writing, software
	* distributed under the License is distributed on an "AS IS" BASIS,
	* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	* See the License for the specific language governing permissions and
	* limitations under the License.
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

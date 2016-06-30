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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.educationaldatamining.spark.ml.hmm.LogProb

/**
	* A Bayesian Knowledge Tracing model
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/14/16.
	*/
class BKTModel(override val uid: String)
	extends Model[BKTModel] with BKTParams with BKTColumnParams
{
	/** Setters for BKT parameters **/

	/** @group setParam **/
	def setPInit( value: Double ): BKTModel = set(pInit, value)
	setDefault( pInit -> 0.0 )

	/** @group setParam **/
	def setPLearn( value: Double ): BKTModel = set(pLearn, value)
	setDefault( pLearn -> 0.0 )

	/** @group setParam **/
	def setPGuess( value: Double ): BKTModel = set(pGuess, value)
	setDefault( pGuess -> 0.0 )

	/** @group setParam **/
	def setPSlip( value: Double ): BKTModel = set(pSlip, value)
	setDefault( pSlip -> 0.0 )

	/** Setters for column names **/

	/** @group setParam **/
	def setStudentResultsCol( value: String ): BKTModel = set( resultsCol, value )

	/** @group setParam **/
	def setPCorrectCol( value: String ): BKTModel = set(pCorrectCol, value)
	setDefault( pCorrectCol -> "PCorrect" )

	/** @group setParam **/
	def setPKnownCol( value: String ): BKTModel = set(pKnownCol, value)
	setDefault( pKnownCol -> "PKnown" )

	/** PCorrect & PKnown implementations **/

	private[bkt] def isCorrect( value: Double ):Boolean =
	{
		value match {
			case (1.0) => true
			case (0.0) => false
			case _ => throw new IllegalArgumentException("Results values must be either 0.0 or 1.0")
		}
	}

	/**
		* Update the PKnown estimate based on observed behavior
		*
		* @param correct whether or not the observed behavior was correct
		* @param prior the prior estimate of PKnown
		*/
	private[bkt] def updatePKnown( correct: Boolean, prior: Double ): Double =
	{
		// log-transform some values
		val priorKnown = new LogProb(prior)
		val priorUnknown = LogProb.ONE - priorKnown
		val guessed = new LogProb(getPGuess)
		val slipped = new LogProb(getPSlip)

		// probability that the student knows the skill, given the observation
		// depends on whether or not they slipped
		val pKnewIt = correct match {
			case true => priorKnown * (LogProb.ONE - slipped) // student knew it and didn't slip
			case false => priorKnown * slipped // student knew the answer, but slipped
		}

		// probabilty that the student didn't know the skill, given the observation
		// hinges on whether or not they guessed
		val pDidntKnowIt = correct match {
			case true => priorUnknown * guessed // student didn't know it, but guessed
			case false => priorUnknown * (LogProb.ONE - guessed) // didn't know it, and didn't guess
		}

		// what's the full probabilty that they knew the skill?
		val posteriorKnown = pKnewIt / ( pKnewIt + pDidntKnowIt )

		// and then there's some chance that they just learned it now
		( posteriorKnown + (LogProb.ONE - posteriorKnown) * new LogProb(getPLearn)).asProb
	}

	/**
		* Calculate the sequence of pKnown probabilities for the observed sequence
		*
		* @param observed
		* @return
		*/
	private[bkt] def pKnown( observed: Seq[Double] ): Seq[Double] =
	{
		// start with our initial probability of knowledge
		val pK = new Array[Double]( observed.length + 1 )
		pK(0) = getPInit
		// update pKnown based o prior value and observed
		for( i <- observed.indices )
			pK(i + 1) = updatePKnown( isCorrect(observed(i)), pK(i) )
		pK
	}

	/**
		*
		* @param pKnown
		* @return
		*/
	private[bkt] def pCorrect( pKnown: Seq[Double] ): Seq[Double] =
	{
		val noSlip = LogProb.ONE - new LogProb(getPSlip)
		val guessed = new LogProb(getPGuess)

		// correct behavior comes from knowing and not slipping
		// or not knowing, but successfully guessing
		def pC( pK: LogProb ): Double =
			( ( pK * noSlip ) + ( (LogProb.ONE - pK) * guessed ) ).asProb

		// apply it to all pKnown values
		pKnown.map( new LogProb(_) ).map( pC(_) )
	}

	/** implement Model methods **/

	override def copy(extra: ParamMap): BKTModel =
	{
		val newModel = copyValues( new BKTModel(uid), extra )
		newModel.setParent(parent)
	}

	@DeveloperApi
	override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

	override def transform(dataset: DataFrame): DataFrame =
	{
		transformSchema( dataset.schema, true )
		// throw warning if PCorrect, PKnown not set
		if( $(pCorrectCol).isEmpty || $(pKnownCol).isEmpty ){
			logWarning( uid +": BKTModel.transform was called as a NOOP since the pCorrect or pKnown column names are not set!")
			dataset
		}
		else {
			// run the model on the opportunities
			val pkUDF = udf { (res: Seq[Double] ) => pKnown( res ) }
			val pcUDF = udf { ( pK: Seq[Double] ) => pCorrect( pK ) }
			dataset.withColumn( $(pKnownCol), pkUDF( col($(resultsCol)) ) )
				     .withColumn( $(pCorrectCol), pcUDF( col($(pKnownCol)) ) )
		}
	}
}

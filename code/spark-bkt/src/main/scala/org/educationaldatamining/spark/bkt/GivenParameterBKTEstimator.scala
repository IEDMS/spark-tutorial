package org.educationaldatamining.spark.bkt

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
	* A simple Estimator implementation for BKT. It simple returns a
	* BKTModel with the parameters set on the estimator. This should be used
	* in conjunction with a cross-validated grid-search in order to fit
	* the parameters.
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/28/16.
	*/
class GivenParameterBKTEstimator(override val uid: String)
	extends Estimator[BKTModel] with BKTParams with BKTColumnParams
{
	/**
		* No-Arg constructor
		* @return
		*/
	def this() = this( Identifiable.randomUID("SimpleBKTEstimator") )

	/** Setters for BKT parameters **/

	/** @group setParam **/
	def setPInit( value: Double ): GivenParameterBKTEstimator = set(pInit, value)
	setDefault( pInit -> 0.0 )

	/** @group setParam **/
	def setPLearn( value: Double ): GivenParameterBKTEstimator = set(pLearn, value)
	setDefault( pLearn -> 0.0 )

	/** @group setParam **/
	def setPGuess( value: Double ): GivenParameterBKTEstimator = set(pGuess, value)
	setDefault( pGuess -> 0.0 )

	/** @group setParam **/
	def setPSlip( value: Double ): GivenParameterBKTEstimator = set(pSlip, value)
	setDefault( pSlip -> 0.0 )

	/** Setters for column names **/

	/** @group setParam **/
	def setStudentResultsCol( value: String ): GivenParameterBKTEstimator = set( resultsCol, value )

	/** @group setParam **/
	def setPCorrectCol( value: String ): GivenParameterBKTEstimator = set(pCorrectCol, value)
	setDefault( pCorrectCol -> "PCorrect" )

	/** @group setParam **/
	def setPKnownCol( value: String ): GivenParameterBKTEstimator = set(pKnownCol, value)
	setDefault( pKnownCol -> "PKnown" )

	override def fit(dataset: DataFrame): BKTModel =
		new BKTModel(uid)
			.setPInit( getPInit )
			.setPLearn( getPLearn )
			.setPGuess( getPGuess )
			.setPSlip( getPSlip )

	@DeveloperApi
	override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

	override def copy(extra: ParamMap): Estimator[BKTModel] =
		copyValues( new GivenParameterBKTEstimator(uid), extra )
}

package org.educationaldatamining.spark.bkt

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	* except segments copied/inspired from
	* RegressionEvaluator.scala (revision 9e82273) available here:
	* https://github.com/apache/spark/blob/v1.6.1/mllib/src/main/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.scala
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*
	* Segments copied/inspired from RegressionEvaluator are licensed under the Apache License, 2.0 as per the following:
	*
	* Licensed to the Apache Software Foundation (ASF) under one or more
	* contributor license agreements.  See the NOTICE file distributed with
	* this work for additional information regarding copyright ownership.
	* The ASF licenses this file to You under the Apache License, Version 2.0
	* (the "License"); you may not use this file except in compliance with
	* the License.  You may obtain a copy of the License at
	*
	*    http://www.apache.org/licenses/LICENSE-2.0
	*
	* Unless required by applicable law or agreed to in writing, software
	* distributed under the License is distributed on an "AS IS" BASIS,
	* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	* See the License for the specific language governing permissions and
	* limitations under the License.
	*/


import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

/**
	* Evaluator for BKT results
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/26/16.
	*/
class BKTEvaluator(override val uid: String)
	extends Evaluator with BKTColumnParams
{
	/**
		* No-Arg constructor
		*/
	def this() = this( Identifiable.randomUID("BKTEvaluator") )

	/**
		* name of the metric to use to evaluate fit
		*/
	val metricName: Param[String] =
	{
		val allowedParams = ParamValidators.inArray( Array("mse", "rmse", "r2", "mae") )
		new Param( this, "metricName", "metric name in evaluation (mse|rmse|r2|mae)", allowedParams)
	}
	setDefault( metricName -> "rmse" )

	/** @group getParam **/
	def getMetricName: String = $(metricName)

	/** Setters for column names **/

	/** @group setParam **/
	def setMetricName( value: String ): BKTEvaluator = set( metricName, value )

	/** @group setParam **/
	def setStudentResultsCol( value: String ): BKTEvaluator = set( resultsCol, value )

	/** @group setParam **/
	def setPCorrectCol( value: String ): BKTEvaluator = set(pCorrectCol, value)
	setDefault( pCorrectCol -> "PCorrect" )

	/** @group setParam **/
	def setPKnownCol( value: String ): BKTEvaluator = set(pKnownCol, value)
	setDefault( pKnownCol -> "PKnown" )

	/** evaluation! **/

	override def isLargerBetter: Boolean =
		$(metricName) match {
			case "rmse" => false
			case "mse" => false
			case "r2" => true
			case "mae" => false
		}

	override def evaluate(dataset: DataFrame): Double =
	{
		// first let's validate the table schema
		val schema = dataset.schema
		// make sure the schema provides the specified opps column
		require( schema.fieldNames.contains( $( resultsCol ) ), "The DataFrame must have a column named "+ $( resultsCol ) )
		val colType = schema($( resultsCol ) ).dataType
		require( colType.equals( resultsType ),
		         "Column "+ $( resultsCol ) +" must be of type "+ resultsType +", but is actually "+ colType )
		// make sure we have the pCorrect column
		require( schema.fieldNames.contains( $( pCorrectCol ) ), "The DataFrame must have a column named "+ $( pCorrectCol ) )
		val pCorType = schema( $(pCorrectCol) ).dataType
		require( pCorType.equals( pCorrectType ),
		         "Column "+ $(pCorrectCol) +" must be of type "+ pCorrectType +", but is actually "+ pCorType )

		// ok - now let's evaluate our pCorrect estimates
		// first unpack each sequence of boolean observations and Double pCorrect values
		// and line them up in (predicted, lable) pairs
		val bool2Dbl = udf{ (res: Seq[Boolean]) => res.map( if(_) 1.0 else 0.0 ) }
		val predLab = dataset.select( col($(pCorrectCol)), bool2Dbl( col($(resultsCol)) ) )
			.flatMap( { case Row( pCor: Seq[Double], res: Seq[Double] ) => pCor.zip(res) } )
		// instantiate regression metrics instance:
		val metrics = new RegressionMetrics(predLab)
		// return the named metric
		$(metricName) match {
			case "rmse" => metrics.rootMeanSquaredError
			case "mse" => metrics.meanSquaredError
			case "r2" => metrics.r2
			case "mae" => metrics.meanAbsoluteError
		}
	}

	def copy(extra: ParamMap): Evaluator = defaultCopy(extra)
}

package org.educationaldatamining.tutorials.spark

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

import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.educationaldatamining.spark.bkt.{BKTEvaluator, BKTModel, GivenParameterBKTEstimator}

/**
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/25/16.
	*/
object EvaluateBKT
{
	def main(args: Array[String])
	{
		// set up our spark context
		val sc = new SparkContext
		val sqlContext = new SQLContext(sc)
		// import implicit functions defined for SQL
		import sqlContext.implicits._

		// load the data
		val kc = sqlContext.read.load("./data/kc_CTA1_01-4")
		val tx = sqlContext.read.load("./data/tx_CTA1_01-4")

		// show the KCs
		kc.select( $"KC" ).distinct.show(false)

		// get the transactions for a given skill
		val findy = kc.filter( $"KC" === "Find Y, positive slope-1" )
			.join( tx, "Transaction_ID" )

		// how many transactions do we have?
		findy.count()

		// get the first-attempts for each student-problem-step
		val first_attempts = findy.filter( $"Attempt_At_Step" === 1 )
			.select( $"Level_Section".as("Section"),
			         $"KC",
			         $"Anon_Student_Id".as("Student"),
			         $"Time",
			         $"Problem_Name".as("Problem"),
			         $"Step_Name".as("Step"),
			         $"Outcome",
			         when( $"Outcome" === "OK", 1.0 ).otherwise(0.0).as("Result") )
			.orderBy( $"Student", $"Time" )

		// have a look at our first attempts!
		first_attempts.show(false)

		// how many first attempts do we have?
		first_attempts.count()

		// aggregate the outcomes into a vector of doubles:
		val studentResults = first_attempts.groupBy( $"Section", $"KC", $"Student" )
			.agg( collect_list($"Result").as("Results") ).cache

		// let's look at these!
		studentResults.show(false)

		// Let's set up a BKT model with some reasonable parameters
		val bkt = new BKTModel( Identifiable.randomUID("Find Y BKT") )
			.setStudentResultsCol("Results")
			.setPInit( 0.3 )
			.setPLearn( 0.2 )
			.setPGuess( 0.15 )
			.setPSlip( 0.2 )

		// run BKT on the data
		val predicted = bkt.transform( studentResults )

		// have a look
		predicted.show(false)

		// evaluate the model
		val bktEval = new BKTEvaluator().setStudentResultsCol("Results")
		println( bktEval.getMetricName +" for the BKT model is: "+ bktEval.evaluate(predicted) )

		// let's do a brute-force grid-search
		val pGrid = new ParamGridBuilder()
			.addGrid( bkt.pInit, 0.1 until 1.0 by 0.1 )
			.addGrid( bkt.pLearn, 0.1 until 1.0 by 0.1 )
			.addGrid( bkt.pGuess, 0.1 until 0.5 by 0.1 )
			.addGrid( bkt.pSlip, 0.1 until 0.5 by 0.1 )

		// set up our estimator
		val bktEst = new GivenParameterBKTEstimator().setStudentResultsCol("Results")

		// and some cross-validation
		val cv = new CrossValidator()
			.setEstimator(bktEst)
			.setEvaluator(bktEval)
			.setEstimatorParamMaps(pGrid.build())
			.setNumFolds(4)

		// let's do an 80-20 training/test split
		val Array( train, test ) = studentResults.randomSplit( Array( 0.8, 0.2 ) )

		// fitting the cross-validator will now run a grid-search over BKT parameters
		val bfModel = cv.fit( train )

		// let's see how we did on our test set
		println( bktEval.getMetricName +" of our test set was: "+ bktEval.evaluate(bfModel.transform( test )) )

		// shut down our spark context
		sc.stop
	}
}

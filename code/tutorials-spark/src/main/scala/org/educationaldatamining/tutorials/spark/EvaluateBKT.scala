package org.educationaldatamining.tutorials.spark

import org.apache.spark.SparkContext
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.educationaldatamining.spark.bkt.{BKTEvaluator, BKTModel}

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
			         when( $"Outcome" === "OK", true ).otherwise(false).as("Result") )
			.orderBy( $"Student", $"Time" )

		// have a look at our first attempts!
		first_attempts.show(false)

		// how many first attempts do we have?
		first_attempts.count()

		// aggregate the outcomes into a vector of doubles:
		val studentResults = first_attempts.groupBy( $"Section", $"KC", $"Student" ).agg( collect_list($"Result").as("Results") )

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
		bktEval.evaluate(predicted)

		// shut down our spark context
		sc.stop
	}
}

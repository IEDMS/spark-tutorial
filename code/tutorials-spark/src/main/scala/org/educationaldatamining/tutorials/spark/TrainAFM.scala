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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.ListBuffer

/**
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/26/16.
	*/
object TrainAFM
{
	def main(args: Array[String]): Unit =
	{
		// set up our spark context
		val sc = new SparkContext
		val sqlContext = new SQLContext(sc)
		// import implicit functions defined for SQL
		import sqlContext.implicits._

		// load the data
		val kc = sqlContext.read.load("./data/kc_CTA1_01-4")
		val tx = sqlContext.read.load("./data/tx_CTA1_01-4")

		// get our first-attempts
		val first_attempts = tx.filter( $"Attempt_At_Step" === 1 )
			.select( $"Transaction_Id",
			         $"Level_Section".as("Section"),
			         $"Anon_Student_Id".as("Student"),
			         $"Time",
			         $"Problem_Name".as("Problem"),
			         $"Step_Name".as("Step"),
			         $"Outcome",
			         when( $"Outcome" === "OK", 1.0 ).otherwise(0.0).as("Result") )
			.orderBy( $"Student", $"Time" ).cache

		// find the # of practice attempts per KC (per student)
		val practice_per_KC = first_attempts.join( kc, "Transaction_Id" )
			.select( $"Student",
			         concat_ws("_", $"Section", $"KC" ).as("Section-KC"),
			         $"Transaction_Id",
			         $"Time",
			         $"Result",
			         rank().over( Window.partitionBy("Student","Section","kc")
				                      .orderBy("Time")
			                    ).as( "Practice" )
			       )

		// create a feature-vector per KC
		val kcIndexer = new StringIndexer()
			.setInputCol("Section-KC")
			.setOutputCol("KC_Idx")
			.fit(practice_per_KC)
		val kcEncoder = new OneHotEncoder()
			.setDropLast(false)
			.setInputCol("KC_Idx")
			.setOutputCol("KC_Feature")
		// get a kc-feature per transaction
		val kcenc = kcEncoder.transform( kcIndexer.transform(practice_per_KC) )

		// we need a new feature which is the (# of practice opportunities) x (the kc feature vector)
		val dot = udf{
			             ( a: Int, v: Vector ) =>
			             {
				             val l = new ListBuffer[(Int,Double)]
				             v.foreachActive( (i,d) => l.append( ( i, a*d ) )  )
				             Vectors.sparse( v.size, l )
			             }
		             }
		val practice_feature = kcenc.withColumn( "Practice_Feature", dot( $"Practice", $"KC_Feature" ) )

		// now we aggregate our KC feature vectors up to the transaction level
		val kcVectorAdder = new VectorSum( kcIndexer.labels.length )
		val features_per_tx = practice_feature
			.groupBy( $"Transaction_Id", $"Student", $"Result" )
			.agg( kcVectorAdder( $"KC_Feature" ).as("KC_Feature" ),
			      kcVectorAdder( $"Practice_Feature" ).as("Practice_Feature") )

		// create a feature-vector per Student
		val studentNameIndexer = new StringIndexer()
			.setInputCol("Student")
			.setOutputCol("Student_Idx")
		val studentFeatureEncoder = new OneHotEncoder()
			.setDropLast(false)
			.setInputCol("Student_Idx")
			.setOutputCol("Student_Feature")

		// assemble a pipeline for the students
		val studentPM = new Pipeline()
			.setStages( Array( studentNameIndexer, studentFeatureEncoder ) )
			.fit(features_per_tx)

		// generate our student features
		val stuenc = studentPM.transform(features_per_tx)

		// assemble the final feature vector from all others:
		val va = new VectorAssembler()
			.setInputCols( Array("Student_Feature","KC_Feature","Practice_Feature") )
			.setOutputCol("Features")
		val afm_data = va.transform( stuenc ).select( $"Transaction_Id", $"Features", $"Result" ).cache

		// let's set aside a 10% test set
		val Array(train, test) = afm_data.randomSplit( Array( 0.9, 0.1 ) )

		// train the logistic regression model!
		val lr = new LogisticRegression()
			.setMaxIter(10)
			.setFitIntercept(false)
			.setFeaturesCol("Features")
			.setLabelCol("Result")
			.setRawPredictionCol("LRraw")
			.setProbabilityCol("LRprob")
			.setPredictionCol("LRpred")

		// train our model
		val afm_model = lr.fit( train )

		// apply the model to the test data:
		val predictions = afm_model.transform(test)

		// how good is this model?
		val bcEval = new BinaryClassificationEvaluator().setLabelCol("Result").setRawPredictionCol("LRraw")
		println( bcEval.getMetricName +" for our AFM model is: "+ bcEval.evaluate(predictions) )
	}
}

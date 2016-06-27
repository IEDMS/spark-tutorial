package org.educationaldatamining.tutorials.spark

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SQLContext

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

		// transform our KCs
		val kcIndexer = new StringIndexer()
			.setInputCol("kc")
			.setOutputCol("kc_Idx")
			.fit(kc)
		val kcEncoder = new OneHotEncoder()
			.setDropLast(false)
			.setInputCol("kc_Idx")
			.setOutputCol("kc_Feature")
		// get a kc-feature per transaction
		val kcVectorAdder = new VectorSum( kcIndexer.labels.length )
		val kcenc = kcEncoder.transform( kcIndexer.transform(kc) )
		val kcFeaturesByTx = kcenc.groupBy( $"Transaction_Id" ).agg( kcVectorAdder( $"kc_Feature" ).as("kc_Feature") )
		// join it with the transactions
		val txWkc = tx.join( kcFeaturesByTx, "Transaction_Id" ).cache

		// create some transformers to encode our data
		val studentNameIndexer = new StringIndexer()
			.setInputCol("Anon_Student_Id")
			.setOutputCol("Student_Idx")
			.fit(tx)
		val studentFeatureEncoder = new OneHotEncoder()
			.setDropLast(false)
			.setInputCol("Student_Idx")
			.setOutputCol("Student_Feature")

		// assemble everything into a pipeline
		val pipeline = new Pipeline()
			.setStages( Array( studentNameIndexer, studentFeatureEncoder ) )
	}
}

package org.educationaldatamining.tutorials.spark

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/23/16.
	*/
object LoadDataShop
{
	def main(args: Array[String])
	{
		// set up our spark context
		val sc = new SparkContext
		val sqlContext = new SQLContext(sc)

		// import implicit functions defined for SQL
		import sqlContext.implicits._

		// load the datashop sample
		val ds607 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter","\t").option("quote", null).load("./data/ds607_tx_CTA1_01-4.tsv")

		// look at the schema
		ds607.printSchema()

		// ugh - skills are scattered across several columns!
		// let's restore some relational sanity to this data...

		// lets first break out the KTraced skills into separate dataframes:
		val kc1 = ds607.select( $"Transaction ID".as("Transaction_ID"), $"KC (KTracedSkills)-1".as("kc") )
		val kc2 = ds607.select( $"Transaction ID".as("Transaction_ID"), $"KC (KTracedSkills)-2".as("kc") )
		val kc3 = ds607.select( $"Transaction ID".as("Transaction_ID"), $"KC (KTracedSkills)-3".as("kc") )
		// now merge them all together:
		val kc = kc1.unionAll( kc2 ).unionAll( kc3 ).filter( $"kc" !== "" ).distinct

		// how many KCs do we have?
		kc.select( $"kc" ).distinct.count

		// ok - let's drop some columns we don't care about:
		val dropcols = Seq( "Row", "Sample Name", "Student Response Type", "Student Response Subtype", "Tutor Response Subtype")
		val keepcols = ds607.columns.filter( colname => ! colname.startsWith("KC") && ! dropcols.contains( colname ) )
		// we'll need to get rid of illegal column characters
		val fixcols = keepcols.map( str => (str, str.replaceAll("\\s+","_").replaceAll("[\\(\\)]","") ) )

		// our transactions!
		val tx = ds607.select( fixcols.map( fix => ds607.col(fix._1).as(fix._2) ): _* )

		// let's have a look at our new schema:
		tx.printSchema()

		// save to parquet files
		kc.write.save("./data/kc_CTA1_01-4")
		tx.write.save("./data/tx_CTA1_01-4")

		// shut down our spark context
		sc.stop
	}
}

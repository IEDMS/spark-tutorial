package org.educationaldatamining.spark.datashop

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
	* Loads Transactions from a DataShop Export file
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/26/16.
	*/
class DataShopTransactionLoader( sql: SQLContext, exportFileName: String )
{
	import sql.implicits._

	/**
		* The exported data
		*/
	private val export = sql.read.format( "com.databricks.spark.csv" )
		.option( "header", "true" )
		.option( "inferSchema", "true" )
		.option( "delimiter", "\t" )
		.option( "quote", null )
		.load( exportFileName )

	// the KC columns
	private val kccols = export.columns.filter( _.startsWith("KC (") )


	// correct column names
	private def standardizeColName( colName: String ) = colName.replaceAll("\\s+","_").replaceAll("[\\(\\)]","")

	/**
		* Get the transactions
		* @return a DataFrame containing the transactions, without KC columns
		*/
	def getTransactions: DataFrame =
	{
		val selectCols = export.columns.filter( ! _.startsWith("KC") )
		export.select( selectCols.map( colName => export.col(colName).as( standardizeColName(colName) ) ): _* )
	}
}

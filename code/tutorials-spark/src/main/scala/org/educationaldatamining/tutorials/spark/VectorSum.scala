package org.educationaldatamining.tutorials.spark

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
	* An aggregate function for summing vectors
	* Thanks to Ritesh Argawal for providing this useful guide:
	* https://ragrawal.wordpress.com/2015/11/03/spark-custom-udaf-example/
	*
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/27/16.
	*/
class VectorSum( vectorLength: Int ) extends UserDefinedAggregateFunction
{
	private def vectorSum( a: Vector, b: Vector ): Vector =
		Vectors.dense( a.toArray.zip( b.toArray ).map( ab => ab._1 + ab._2 ) )

	override def inputSchema: StructType = new StructType(Array( StructField( "vector", new VectorUDT) ) )

	override def dataType: DataType = new VectorUDT

	override def bufferSchema: StructType = new StructType(Array( StructField( "sum", new VectorUDT) ) )

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
	{
		buffer(0) = vectorSum( buffer.getAs[Vector](0), input.getAs[Vector](0) )
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
	{
		buffer1(0) = vectorSum( buffer1.getAs[Vector](0), buffer2.getAs[Vector](0) )
	}

	override def initialize(buffer: MutableAggregationBuffer): Unit =
	{
		buffer(0) = Vectors.zeros(vectorLength)
	}

	override def deterministic: Boolean = true

	override def evaluate(buffer: Row): Any = buffer.getAs[Vector](0)
}

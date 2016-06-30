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

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.types._

/**
	* Columns for BKT models
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/26/16.
	*/
trait BKTColumnParams extends Params
{
	/** Input column **/

	/**
		* Results Column
		* @group param
		*/
	final val resultsCol: Param[String] = new Param[String]( this, "resultsCol", "Name of the column containing the student results data" )
	protected final val resultsType = new ArrayType( DoubleType, true )

	/** Output columns: predicted values **/

	/**
		* P-Correct results
		* @group param
		*/
	final val pCorrectCol: Param[String] = new Param[String]( this, "pCorrectCol", "Name of the column where pCorrect values will be stored" )
	protected final val pCorrectType = new ArrayType( DoubleType, false )

	/**
		* P-Known results
		* @group param
		*/
	final val pKnownCol: Param[String] = new Param[String]( this, "pKnownCol", "Name of the column where pKnown values will be stored" )
	protected final val pKnownType = new ArrayType( DoubleType, false )

	/** Getters for params **/

	/** @group getParam **/
	final def getStudentResultsCol = $( resultsCol )

	/** @group getParam **/
	final def getPCorrectCol = $(pCorrectCol)

	/** @group getParam **/
	final def getPKnownCol = $(pKnownCol)

	/**
		*
		* @param schema
		* @return
		*/
	protected def validateAndTransformSchema(schema: StructType): StructType =
	{
		// make sure the schema provides the specified opps column
		require( schema.fieldNames.contains( $( resultsCol ) ), "The DataFrame must have a column named "+ $( resultsCol ) )
		val colType = schema($( resultsCol ) ).dataType
		require( colType.equals( resultsType ),
		         "Column "+ $( resultsCol ) +" must be of type "+ resultsType +", but is actually "+ colType )
		// add the result columns
		require( !schema.fieldNames.contains($(pCorrectCol)), "Result column "+ $(pCorrectCol) +" already exists!")
		require( !schema.fieldNames.contains($(pKnownCol)), "Result column "+ $(pKnownCol) +" already exists!")
		StructType( schema.fields :+
			            StructField( $(pKnownCol), pKnownType ) :+
			            StructField( $(pCorrectCol), pCorrectType ) )
	}
}

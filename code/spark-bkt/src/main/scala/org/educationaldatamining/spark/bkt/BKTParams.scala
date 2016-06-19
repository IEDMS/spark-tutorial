package org.educationaldatamining.spark.bkt

import com.sun.tools.classfile.Type.ArrayType
import org.apache.spark.ml.param._
import org.apache.spark.sql.types.{StructField, StructType}

/**
	* Copyright (C) 2016 Tristan Nixon <tristan.m.nixon@gmail.com>
	*
	* This work is licensed under the Creative Commons Attribution-ShareAlike 4.0 International License.
	* To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/4.0/.
	*
	* This legend must continue to appear in the source code despite modifications or enhancements by any party.
	*/

/**
	* Parameters for Bayesian Knowledge Tracing
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/18/16.
	*/
trait BKTParams extends Params
{
	/** BKT parameters **/

	/**
		* P-Init parameter
		* @group param
		*/
	final val pInit: DoubleParam = new DoubleParam( this, "pInit",
	                                                "Probability of initial student knowledge",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Learn parameter
		* @group param
		*/
	final val pLearn: DoubleParam = new DoubleParam( this, "pLearn",
	                                                 "Probability of learning",
	                                                 ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Guess parameter
		* @group param
		*/
	final val pGuess: DoubleParam = new DoubleParam( this, "pGuess",
	                                                "Probability of guessing",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/**
		* P-Slip parameter
		* @group param
		*/
	final val pSlip: DoubleParam = new DoubleParam( this, "pSlip",
	                                                "Probability of making a mistake (slipping)",
	                                                ParamValidators.inRange( 0.0, 1.0 ) )

	/** Input columns: opportunity values **/

	final val oppsCol: Param[String] = new Param[String]( this, "oppsCol", "Name of the column containing the student opportunity data")

	/** Output columns: predicted values **/

	/**
		* P-Correct results
		* @group param
		*/
	final val pCorrectCol: Param[String] = new Param[String]( this, "pCorrectCol", "Name of the column where pCorrect values will be stored" )

	/**
		* P-Known results
		* @group param
		*/
	final val pKnownCol: Param[String] = new Param[String]( this, "pKnownCol", "Name of the column where pKnown values will be stored" )

	/** Getters for the parameters **/

	/** @group getParam **/
	final def getPInit = $(pInit)

	/** @group getParam **/
	final def getPLearn = $(pLearn)

	/** @group getParam **/
	final def getPGuess = $(pGuess)

	/** @group getParam **/
	final def getPSlip = $(pSlip)

	/** @group getParam **/
	final def getOppsCol = $(oppsCol)

	/** @group getParam **/
	final def getPCorrectCol = $(pCorrectCol)

	/** @group getParam **/
	final def getPKnownCol = $(pKnownCol)

	/**
		* Validates and transforms the input schema with the provided param map.
		* @param schema the input schema
		* @return the output schema
		*/
	protected def validateAndTransformSchema( schema: StructType ): StructType =
	{
		// make sure the schema provides the specified opps column
		require( schema.fieldNames.contains( $(oppsCol) ), "The DataFrame must have a column named "+ $(oppsCol) )
		val colType = schema($(oppsCol)).dataType
		require( colType.equals( ArrayType[Boolean] ),
		         "Column "+ $(oppsCol) +" must be of type Array[Boolean], but is actually "+ colType )
		// add the result columns
		require( !schema.fieldNames.contains($(pCorrectCol)), "Result column "+ $(pCorrectCol) +" already exists!")
		require( !schema.fieldNames.contains($(pKnownCol)), "Result column "+ $(pKnownCol) +" already exists!")
		StructType( schema.fields :+
			          StructField( $(pCorrectCol), ArrayType[Double], false ) :+
			          StructField( $(pKnownCol), ArrayType[Double], false ) )
	}
}

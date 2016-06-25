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

/**
	* It's a whale of a good time!
	* <p/>
	* Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 6/14/16.
	*/
object HelloSpark
{
	def main( args: Array[String] )
	{
		// set up our spark context
		val sc = new SparkContext

		// load moby dick
		val md = sc.textFile("./data/Moby Dick.txt")

		// how many lines do we have?
		md.count()

		// what's the first line say?
		md.first

		// let's take a quick look at the first 20 lines
		md.take(20)

		// how many lines mention the narrator, Ishmael?
		md.filter( line => line.contains("Ishmael") ).count

		// let's look at them:
		md.filter( _.contains("Ishmael") ).collect().addString( new StringBuilder, "\n" )

		// how many lines mention Ahab?
		md.filter( _.contains("Ahab") ).count

		// how many lines mention the whale?
		md.filter( _.toLowerCase.contains("whale") ).count

		// let's count all the words!
		val words = md.flatMap( _.toLowerCase.split("\\W+") ) // this is totally the wrong way to tokenize text, but convenient

		// how many words are there in Moby Dick?
		words.count

		// your first MapReduce implementation!
		val wordCounts = words.map( word => (word,1) ).reduceByKey( (a, b) => a + b ).cache

		// let's peak at some results:
		wordCounts.take(10)

		// how many distinct words do we have?
		wordCounts.count

		// What are the top 10 most frequently used words?
		wordCounts.sortBy( _._2, false ).take(10)

		// shut down our spark context
		sc.stop
	}
}

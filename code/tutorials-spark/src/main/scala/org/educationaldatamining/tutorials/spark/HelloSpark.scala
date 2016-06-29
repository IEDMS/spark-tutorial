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
	def toLines[T]( array: Array[T] ): String = array.addString(new StringBuilder, "\n").toString()

	def main( args: Array[String] )
	{
		// set up our spark context
		val sc = new SparkContext

		// load moby dick
		val md = sc.textFile( args(0) )

		// how many lines do we have?
		println("=> Moby Dick has "+ md.count() +" lines")

		// what's the first line say?
		println("=> It's first line is: '"+ md.first +"'")

		// let's take a quick look at the first 20 lines
		println("=> It's first 20 lines are: "+ toLines( md.take(20) ) )

		// how many lines mention the narrator, Ishmael?
		println("=> 'Ishmael' occurs on "+ md.filter( line => line.contains("Ishmael") ).count +" lines:")

		// let's look at them:
		print( toLines( md.filter( _.contains("Ishmael") ).collect() ) )

		// how many lines mention Ahab?
		println("=> 'Ahab' occurs on "+ md.filter( _.contains("Ahab") ).count +" lines")

		// how many lines mention the whale?
		println("=> 'whale' occurs on "+ md.filter( _.toLowerCase.contains("whale") ).count +" lines")

		// let's count all the words!
		val words = md.flatMap( _.toLowerCase.split("\\W+") ) // this is totally the wrong way to tokenize text, but convenient

		// how many words are there in Moby Dick?
		println("=> Moby Dick has "+ words.count +" words")

		// your first MapReduce implementation!
		val wordCounts = words.map( word => (word,1) ).reduceByKey( (a, b) => a + b ).cache

		// let's peak at some results:
		println("=> Some word counts: "+ toLines( wordCounts.take(10) ) )

		// how many distinct words do we have?
		println("=> Total of "+ wordCounts.count +" distinct words.")

		// What are the top 10 most frequently used words?
		println("=> The top 10 are: "+ toLines( wordCounts.sortBy( _._2, false ).take(10) ) )

		// how many total instances of Ahab are there?
		println("=> 'Ahab' occurs: "+ toLines( wordCounts.filter( wc => wc._1.equalsIgnoreCase("Ahab") ).collect() ) )

		// how many for the whale?
		println("=> 'whale occurs: "+ toLines( wordCounts.filter( wc => wc._1.equalsIgnoreCase("whale") ).collect() ) )

		// shut down our spark context
		sc.stop
	}
}

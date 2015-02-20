/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.xml.{XML, NodeSeq}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object WikipediaPageRankStandalone {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: WikipediaPageRankStandalone <inputFile> <numTop> <numIterations> <university_name>")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()

    val inputFile = args(0)
    val numTop = args(1).toInt
    val numIterations = args(2).toInt
    val university_name = args(3)

    sparkConf.setAppName("WikipediaPageRankStandalone")

    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(inputFile)
    val universities = sc.textFile(university_name).collect.toSet
    val links =
        input.map(parseArticle _).cache()
    val n = links.count()
    val defaultRank = 1.0 / n
    val a = 0.15

    // Do the computation
    val startTime = System.currentTimeMillis
    val ranks =
      pageRank(links, numIterations, defaultRank, a, n)
    val univ = ranks.filter{case (id,rank) => universities.contains(id)}
    // Print the result
    println("Top " + numTop + " :")
//    val top =
//      (ranks
//        .filter { case (id, rank) => rank >= threshold }
//        .map { case (id, rank) => "%s\t%s\n".format(id, rank) }
//        .collect().mkString)
//    println(top)
    val top = (univ.top(numTop) {
                  Ordering.by((entry:(String, Double)) => entry._2)
                }.map { case (id, rank) => "%s\t%s\n".format(id, rank) }.mkString)

    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
      .format(numIterations, time, time / numIterations))
    sc.stop()
  }

  def parseArticle(line: String): (String, Array[String]) = {
    val fields = line.split("\t")
    val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
    val id = new String(title)
    val links =
      if (body == "\\N") {
        NodeSeq.Empty
      } else {
        try {
          XML.loadString(body) \\ "link" \ "target"
        } catch {
          case e: org.xml.sax.SAXParseException =>
            System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
            NodeSeq.Empty
        }
      }
    val outEdges = links.map(link => new String(link.text)).toArray
    (id, outEdges)
  }

  def pageRank(
                links: RDD[(String, Array[String])],
                numIterations: Int,
                defaultRank: Double,
                a: Double,
                n: Long
                ): RDD[(String, Double)] = {
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (linksWrapperIterable, rankWrapperIterable)) =>
          val linksWrapper = linksWrapperIterable.iterator
          val rankWrapper = rankWrapperIterable.iterator
          if (linksWrapper.hasNext) {
            val linksWrapperHead = linksWrapper.next
            if (rankWrapper.hasNext) {
              val rankWrapperHead = rankWrapper.next
              linksWrapperHead.map(dest => (dest, rankWrapperHead / linksWrapperHead.size))
            } else {
              linksWrapperHead.map(dest => (dest, defaultRank / linksWrapperHead.size))
            }
          } else {
            Array[(String, Double)]()
          }
      }
      ranks = (contribs.combineByKey((x: Double) => x,
                                     (x: Double, y: Double) => x + y,
                                     (x: Double, y: Double) => x + y)
               .mapValues(sum => a/n + (1-a)*sum))
    }
    ranks
  }
}


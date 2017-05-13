package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /** 
    Given a patient ID, compute the random walk probability w.r.t. to all other patients. 
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */

    /** Remove this placeholder and implement your code */
    //List(1,2,3,4,5)
    //val personalized = patientID
    val src: VertexId = patientID

    var rankGraph: Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src ).mapVertices { (id, attr) => if (!(id != src)) alpha else 0.0}

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iter = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iter < numIter) {
      rankGraph.cache()
      prevRankGraph = rankGraph
      val updates = rankGraph.aggregateMessages[Double]( ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
      rankGraph = rankGraph.joinVertices(updates) {
        (id, oldRank, msgSum) =>if(id==patientID) alpha + (1.0 - alpha) * msgSum else (1.0 - alpha) * msgSum}.cache()
      rankGraph.edges.foreachPartition(x => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)
      iter += 1
    }
    val top10=rankGraph.vertices.filter(f=>f._1<=1000).takeOrdered(11)(Ordering[Double].reverse.on(x=>x._2)).map(_._1)
    val ans = top10.slice(1,top10.length).toList
    ans
  }
}

/**

students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /** 
    Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients. 
    Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay. The given patientID should be excluded from the result.
    */

    /** Remove this placeholder and implement your code */
    //List(1,2,3,4,5)
    val neighbors=graph.collectNeighborIds(EdgeDirection.Either)
    val filterNeighbor=neighbors.filter(f=> f._1.toLong <= 1000 & f._1.toLong != patientID)
    val setOfNeighbors=neighbors.filter(f=>f._1.toLong==patientID).map(f=>f._2).flatMap(f=>f).collect().toSet
    val patientScore=filterNeighbor.map(f=>(f._1,jaccard(setOfNeighbors,f._2.toSet)))
    patientScore.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2)).map(_._1.toLong).toList
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
    Given a patient, med, diag, lab graph, calculate pairwise similarity between all
    patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where 
    patient-1-id < patient-2-id to avoid duplications
    */

    /** Remove this placeholder and implement your code */
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x=>(x._1,x._2.filter(p=>p>1000))).filter(_._1<=1000)
    val neighborsCart = neighbors.cartesian(neighbors).filter{case(a,b)=>a._1<b._1}
    val similarity = neighborsCart.map{x=>(x._1._1,x._2._1,jaccard(x._1._2.toSet,x._2._2.toSet))}
    val result = similarity.map(x=>(x._3,(x._1,x._2))).sortByKey(false,1).map(x=>(x._2._1,x._2._2,x._1))
    result
    //val sc = graph.edges.sparkContext
    //sc.parallelize(Seq((1L, 2L, 0.5d), (1L, 3L, 0.4d)))
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /** 
    Helper function

    Given two sets, compute its Jaccard similarity and return its result.
    If the union part is zero, then return 0.
    */
    
    /** Remove this placeholder and implement your code */
    val union: Double = (a ++ b).size
    val intersect: Double = a.intersect(b).size
    return (if (union==0) 0.0 else (intersect/union))
  }
}
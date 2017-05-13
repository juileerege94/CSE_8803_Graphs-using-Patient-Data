/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    val sc = patients.sparkContext

    val diag1 = diagnostics.map(diag=>((diag.patientID, diag.icd9code),diag))
    val diagLatest = diag1.reduceByKey((d1,d2)=> if (d1.date>d2.date) d1 else d2).map{case(key, d)=>d}
    val med1 = medications.map(med=>((med.patientID, med.medicine),med))
    val medLatest = med1.reduceByKey((m1,m2)=> if(m1.date>m2.date) m1 else m2).map{case(key, m)=>m}
    val lab1 = labResults.map(lab=>((lab.patientID,lab.labName),lab))
    val labLatest = lab1.reduceByKey((l1,l2)=> if (l1.date>l2.date) l1 else l2).map{case(key, l)=>l}

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients.map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    val startDiag = patients.count()
    val diagVertexRDD = diagLatest.map(_.icd9code).distinct().zipWithIndex().map{case(icd9code, zeroBasedIndex)=>(icd9code,zeroBasedIndex+1001)}
    val diag2VertexID = diagVertexRDD.collect.toMap
    val vertexDiagnostic = diagVertexRDD.map{case(icd9code,index )=>(index, DiagnosticProperty(icd9code))}.asInstanceOf[RDD[(VertexId,VertexProperty)]]

    val startMed = diag2VertexID.size
    val medVertexRDD = medLatest.map(_.medicine).distinct().zipWithIndex().map{case(medicine, zeroBasedIndex)=>(medicine, zeroBasedIndex+1001+startMed)}
    val med2VertexID = medVertexRDD.collect.toMap
    val vertexMedication = medVertexRDD.map{case(medicine,index)=>(index, MedicationProperty(medicine))}.asInstanceOf[RDD[(VertexId,VertexProperty)]]

    val startLab = med2VertexID.size
    val labVertexRDD = labLatest.map(_.labName).distinct().zipWithIndex().map{case(labname, zeroBasedIndex)=>(labname, zeroBasedIndex+1001+startMed+startLab)}
    val lab2VertexID = labVertexRDD.collect.toMap
    val vertexLab = labVertexRDD.map{case(labname, index)=> (index, LabResultProperty(labname))}.asInstanceOf[RDD[(VertexId, VertexProperty)]]

    /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */
    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients.map({p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })

    val diagToVertexID1 = sc.broadcast((diag2VertexID))
    val edgePatientDiag = diagLatest.map(d=>(d.patientID,d.icd9code,d)).map{case(patientID, icd9code, d)=>Edge(patientID.toLong,diagToVertexID1.value(icd9code),PatientDiagnosticEdgeProperty(d).asInstanceOf[EdgeProperty])}
    val edgeDiagPatient = diagLatest.map(d=>(d.patientID,d.icd9code,d)).map{case(patientID, icd9code, d)=>Edge(diagToVertexID1.value(icd9code),patientID.toLong,PatientDiagnosticEdgeProperty(d).asInstanceOf[EdgeProperty])}

    val medToVertexID1 = sc.broadcast((med2VertexID))
    val edgePatientMed = medLatest.map(m=>(m.patientID,m.medicine,m)).map{case(patientID, medicine, m)=>Edge(patientID.toLong,medToVertexID1.value(medicine),PatientMedicationEdgeProperty(m).asInstanceOf[EdgeProperty])}
    val edgeMedPatient = medLatest.map(m=>(m.patientID,m.medicine,m)).map{case(patientID, medicine, m)=>Edge(medToVertexID1.value(medicine),patientID.toLong,PatientMedicationEdgeProperty(m).asInstanceOf[EdgeProperty])}

    val labToVertexID1 = sc.broadcast((lab2VertexID))
    val edgePatientLab = labLatest.map(l=>(l.patientID,l.labName,l)).map{case(patientID, labName, l)=>Edge(patientID.toLong,labToVertexID1.value(labName),PatientLabEdgeProperty(l).asInstanceOf[EdgeProperty])}
    val edgeLabPatient = labLatest.map(l=>(l.patientID,l.labName,l)).map{case(patientID, labName, l)=>Edge(labToVertexID1.value(labName),patientID.toLong,PatientLabEdgeProperty(l).asInstanceOf[EdgeProperty])}

    // Making Graph
    val vertices = sc.union(vertexPatient,vertexDiagnostic,vertexMedication,vertexLab)
    val edgePD = sc.union(edgePatientDiag,edgeDiagPatient)
    val edgePM = sc.union(edgePatientMed,edgeMedPatient)
    val edgePL = sc.union(edgePatientLab,edgeLabPatient)
    val edges = sc.union(edgePD,edgePM,edgePL)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)

    graph
  }
}

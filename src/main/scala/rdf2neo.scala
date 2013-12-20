package rdf2neo

import collection.JavaConverters._
import annotation.tailrec

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream

import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters

import gnu.trove.map.hash.TObjectLongHashMap

import java.io.File

import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider
import org.neo4j.helpers.collection.MapUtil

object Main extends App {
  //clear the existing folder
  for (file <- new java.io.File (Settings.outputGraphPath).listFiles){
      println (file)
      file.delete()
  }
  
  val inserter = BatchInserters.inserter(Settings.outputGraphPath);
  
  //create index 
  val indexProvider = new LuceneBatchInserterIndexProvider( inserter );
  val indexTypeObjectName = indexProvider.nodeIndex("type_object_name", MapUtil.stringMap( "type", "fulltext" ));
  val currentID = Settings.currentDBMaxNodeID + 1;

  var is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  var in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  var instanceCount:Long = 0
  val startTime = System.currentTimeMillis
  var lastTime = System.currentTimeMillis
  val turtleSplit = Array[String]("","","") // reused to avoid GC
  val idMap = new TObjectLongHashMap[String]()
  
  //val setOfPred = collection.mutable.Set[String]()
  
  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, true))

  in.close
  
  is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  in = new BufferedReader(new InputStreamReader(is))

  count = 0
  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, false))
  
  indexProvider.shutdown();
  inserter.shutdown();
  
  println("FINISH!")

  @inline def fastSplit(arr:Array[String], turtle:String):Int = {    
    var c = 0
    var idx = turtle.indexOf('\t')
    if(idx > 0) c += 1
    else return c
    arr(0) = turtle.substring(0, idx).trim()
    var idx2 = turtle.indexOf('\t', idx+1)
    if(idx2 > 0) c += 1
    else return c
    arr(1) = turtle.substring(idx+1, idx2).trim()
    var idx3 = turtle.lastIndexOf('\t')
    arr(2) = turtle.substring(idx2+1, idx3).trim()
    return c+1
  }

  @inline def processTurtle(turtle:String, idOnly:Boolean) = {
    count += 1
    if(count % 10000000 == 0) {
      val curTime = System.currentTimeMillis
      println(count/1000000 + 
        "M turtle lines processed(" +
        (if(idOnly) "first pass" else "second pass") +
        "); elapsed: " + 
        ((curTime - startTime) / 1000) + 
        "s; last 10M: " + 
        ((curTime - lastTime) / 1000) + 
        "s")
      lastTime = curTime
      println("idMap size: " + idMap.size)
    }
    
    if(turtle.startsWith("@base")) {
      // do we need to handle these? 
    } else if (turtle.startsWith("@prefix")) {
      // do we need to handle these?
    } else if (turtle.startsWith("#")) { 
      // definitely don't need to handle these
    } else if (turtle.length > 1) {
      val arrlength = fastSplit(turtleSplit, turtle)
      
      if(arrlength == 3) {
        val (subj, pred, obj) = (turtleSplit(0), turtleSplit(1), turtleSplit(2))
        
        // check if this is a node we want to keep        
        if(idOnly == true && Settings.nodeTypePredicates.contains(pred) 
          && (Settings.nodeTypeSubjects.isEmpty || listStartsWith(Settings.nodeTypeSubjects, subj))){
          //println("setting label: "+turtle)
          //check if it does not in idMap
          if(!idMap.contains(obj)) {
            instanceCount += 1
            idMap.put(obj, new java.lang.Long(instanceCount))
            inserter.createNode(currentID+instanceCount, Map[String,Object]("mid"->obj).asJava) 

            //extract the label
            var label = inserter.getNodeLabels(currentID+instanceCount).asScala.toArray
            label = label :+ DynamicLabel.label(extractLabel(subj))
            label = label :+ DynamicLabel.label("Freebase")
            //println(label.mkString(" "))
            inserter.setNodeLabels(currentID+instanceCount, label : _*)          
      
            //inserter.setNodeLabels(currentID+instanceCount, DynamicLabel.label(extractLabel(subj)))
            //inserter.setNodeLabels(currentID+instanceCount, DynamicLabel.label("Freebase"))
         }
        }else if (idOnly == false && idMap.contains(subj)) { 
          // this is a property/relationship of a node
          val subjId = idMap.get(subj) + currentID
          val sanitizedPred = sanitize(pred)
          
          //check if there is any related object
          if(idMap.contains(obj)) {
            // this is a relationship
            //println("creating relationship: " + sanitizedPred)                        
            val objId = idMap.get(obj) + currentID
            
            inserter.createRelationship(subjId, objId, DynamicRelationshipType.withName(sanitizedPred), null)
          } else {
            // this is a real property
            //println("setting property: " + turtle)
            //if(obj.startsWith("ns:m.")) {
            if (obj.startsWith("<http://rdf.freebase.com/ns/m.")){
              //println("dropping relationship on the ground for an id we don't have: "+turtle)
            } else {
              val trimmedObj = obj.replaceAll("^\"|\"$", "").replace("\"@en", "")
              //println("trimmedObj: " + trimmedObj)
              //println("sanitizedPred: " + sanitizedPred)
              var flag = false
              //need to handle the case about XXX.to>
              // e.g. people.marriage.to
              if((trimmedObj.length > 3 && trimmedObj.substring(trimmedObj.length-3)(0) != '@')
              && (pred.length > 3 && pred.substring(sanitizedPred.length-3)(0) != '_' || pred.endsWith("_en") || pred.endsWith("_to"))) {
                //println ("---->" + sanitizedPred)
                //check if it is related to type.object.key, further investigate
                if (pred == "<http://rdf.freebase.com/ns/type.object.key>"){ 
                  //check if the obj is related to wikipedia
                  if (obj.startsWith("\"/wikipedia/")){
                    //check if it is related to en
                    if (obj.startsWith("\"/wikipedia/en")){
                      flag = true
                    }else{
                      flag = false
                    }                    
                  }else{
                    flag = true
                  }
                }else if(pred == "<http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage>"){
                  //check if it is related to wikipedia
                  if (obj.indexOf("wikipedia.org") > 0){
                    //check if it is related to en
                    if (obj.startsWith("<http://en.wikipedia.org")){
                      flag = true
                    }else{
                    flag = false
                    }
                  }else{
                    flag = true
                  }                  
                }else if (sanitizedPred == "common_topic_description"){
                  if (obj.indexOf("@en") > 0)
                    flag = true
                  else
                    flag = false                  
                }else if (sanitizedPred == "type_object_name"){
                  if (obj.indexOf("@en") > 0)
                    flag = true
                  else
                    flag = false                  
                }else if (sanitizedPred == "common_topic_alias"){
                  if (obj.indexOf("@en") > 0)
                    flag = true
                  else
                    flag = false                  
                }else if (sanitizedPred == "rdf_schema_label"){
                  if (obj.indexOf("@en") > 0)
                    flag = true
                  else
                    flag = false                  
                }
                
                else{
                   //if it is not related, extract it
                     flag = true
                }
                
                //println("flag" +  flag)
                //println
                if (flag == true){
                  //check if it already has the property                
                  if(inserter.nodeHasProperty(subjId, sanitizedPred)) {
                  //println("already has prop: " + subjId + "; pred: "+pred)
                  var prop = inserter.getNodeProperties(subjId).get(sanitizedPred)
                  inserter.removeNodeProperty(subjId, sanitizedPred)
                  //println("got node property: " +subjId + ":"+pred + "; prop: "+prop)
                  
                  prop match {
                    case prop:Array[String] => {
                      //println("prop array detected..."); 
                      inserter.setNodeProperty(subjId, sanitizedPred, prop :+ trimmedObj)
                    }
                    case _ => {
                      //println("converting prop to array..."); 
                      inserter.setNodeProperty(subjId, sanitizedPred, Array[String](prop.toString) :+ trimmedObj)
                    }
                  }
                  
                  } else {
                    //check if it is related to wikipedia
                    if (pred.startsWith("<http://rdf.freebase.com/key/key.wikipedia.")){
                      //check if it is in en, insert if it is
                      if (pred.startsWith("<http://rdf.freebase.com/key/key.wikipedia.en")){
                        inserter.setNodeProperty(subjId, sanitizedPred, trimmedObj)
                      }else{
                       //ignore it 
                      }
                      
                    }else{
                      inserter.setNodeProperty(subjId, sanitizedPred, trimmedObj)
                      
                      //check if it is related to type_object_name
                      //  insert index
                      if (sanitizedPred.equals("type_object_name")){
                        val typeObjectNameProp = MapUtil.map( "type_object_name", trimmedObj )
                        indexTypeObjectName.add(subjId, typeObjectNameProp)
                        indexTypeObjectName.flush()
                      }
                    } 
                  }
                }                
              }
            }
          }
        } else {
          //println("doesn't match filters: " + turtle)
        }
      } else {
        //println("Line split with non-triple: " + turtle)
      }
    }    
  }

  //extractLabel
  // input: String e.g. <http://rdf.freebase.com/ns/people.person>
  // output: String e.g. people  
  def extractLabel(str:String):String = {
   str.substring(str.lastIndexOf("/")+1, str.lastIndexOf("."))
  }
  
  //sanitize
  // input: String e.g. <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
  // output: String e.g. 22_rdf_syntax_ns_type  
  def sanitize(str:String):String = {    
    //str.substring(0, str.length()-1).replaceAll("[^A-Za-z0-9]", "_").replaceAll("_http___", "")
    //println(str)
   str.substring(str.lastIndexOf("/")+1, str.lastIndexOf(">")).replaceAll("[^A-Za-z0-9]", "_")
  }

  @inline def listStartsWith(list:Seq[String], str:String):Boolean = {
    @inline @tailrec def listStartsWith(list:Seq[String], str:String, i:Int):Boolean = {
      if(i >= list.length) {
        false
      } else if(str.startsWith(list(i))) {
        true
      } else {
        listStartsWith(list, str, i+1)
      }
    }

    listStartsWith(list, str, 0)
  }
}

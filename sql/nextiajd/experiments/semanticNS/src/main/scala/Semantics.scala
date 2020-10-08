object Semantics {


  def setNewLabels(containment: Double, cardinalityProportion: Double): Double = {

    if(containment >= 0.75 && cardinalityProportion >= 0.25){ // 4
      return 4
    }

    if(containment >= 0.5 && cardinalityProportion >= 0.125){ // 8
      return 3
    }
    if(containment >= 0.25 && cardinalityProportion >= 0.083){ // 12
      return 2
    }
    if(containment >= 0.1){
      return 1
    }
    return 0
  }
  val newLabelsUDF = udf(setNewLabels(_: Double, _:Double): Double)


  def run(distances: String): Unit = {

    val semantics = spark.read.option("header", "true").option("inferSchema","true")
      .csv(distances)




  }





}
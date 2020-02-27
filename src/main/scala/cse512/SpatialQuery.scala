package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def myContains(queryRectangle:String, pointString:String): Boolean = {
      var rect = queryRectangle.split(',').map(_.toDouble)
      var point = pointString.split(',').map(_.toDouble)
      point(0) >= rect(0) && point(0) <= rect(2) && point(1) >= rect(1) && point(1) <= rect(3) || point(0) >= rect(2) && point(0) <= rect(0) && point(1) >= rect(3) && point(1) <= rect(1)
    }
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(myContains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def myContains(queryRectangle:String, pointString:String): Boolean = {
      var rect = queryRectangle.split(',').map(_.toDouble)
      var point = pointString.split(',').map(_.toDouble)
      point(0) >= rect(0) && point(0) <= rect(2) && point(1) >= rect(1) && point(1) <= rect(3) || point(0) >= rect(2) && point(0) <= rect(0) && point(1) >= rect(3) && point(1) <= rect(1)
    }
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(myContains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def myWithin(pointString1:String, pointString2:String, distance:Double): Boolean = {
      var point1 = pointString1.split(',').map(_.toDouble)
      var point2 = pointString2.split(',').map(_.toDouble)
      (point1(0) - point2(0)) * (point1(0) - point2(0)) + (point1(1) - point2(1)) * (point1(1) - point2(1)) <= distance * distance
    }
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(myWithin(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def myWithin(pointString1:String, pointString2:String, distance:Double): Boolean = {
      var point1 = pointString1.split(',').map(_.toDouble)
      var point2 = pointString2.split(',').map(_.toDouble)
      (point1(0) - point2(0)) * (point1(0) - point2(0)) + (point1(1) - point2(1)) * (point1(1) - point2(1)) <= distance * distance
    }
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(myWithin(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}

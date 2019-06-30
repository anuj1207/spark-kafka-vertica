package edu.technohub.com.models

import edu.technohub.com.utils.Constants.{ID, NAME, AGE}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class Person(id: Int, name: String, age: Int)

object Person{

  //TODO::check::/*Create schema for your model*/
  def schema: StructType =
    StructType(
      List(
        StructField(ID, IntegerType, nullable = true),
        StructField(NAME, StringType, nullable = true),
        StructField(AGE, IntegerType, nullable = true)
      )
    )
}

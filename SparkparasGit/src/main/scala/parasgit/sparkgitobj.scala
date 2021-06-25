package parasgit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object sparkgitobj {
	def main(args:Array[String]):Unit={


			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._
					val jsondf = spark.read.format("json").option("multiLine","true").load("file:///C:/Bigdata/topping.json")
					jsondf.printSchema()
					jsondf.show()

					val flattendf = jsondf.select(
							col("id"),
							col("type"),
							col("name"),
							col("ppu"),
							col("batters.batter.id").alias("batter_id"),
							col("batters.batter.type").alias("batter_type"),
							col("topping.id").alias("topping_id"),
							col("topping.type").alias("topping_type"))
					flattendf.printSchema()
					flattendf.show()

					val complexdf = flattendf.select(
							col("id"),
							col("type"),
							col("name"),
							col("ppu"),
							struct(
									col("batter_id").alias ("id"),
									col("batter_type").alias ("type")
									).alias ("batters"),
							struct(
									col("topping_id").alias ("id"),
									col("topping_type").alias ("type")
									).alias ("topping")
							)
					complexdf.show()
					complexdf.printSchema()

					val jsondf1 = spark.read.format("json").option("multiLine","true").load("file:///C:/Bigdata/arrayjson.json")
					jsondf1.printSchema()
					jsondf1.show()

					val flattendf1 = jsondf1.select(
							"first_name",
							"second_name",
							"Students",
							"address.Permanent_address",
							"address.temporary_address")

					flattendf1.printSchema()
					flattendf1.show()

					val explodedf1= flattendf1.withColumn("Students",explode(col("Students")))
					explodedf1.printSchema()
					explodedf1.show()

					val complexdf2 = explodedf1.groupBy("first_name","second_name","Permanent_address","temporary_address")
					.agg(collect_list("Students").alias("Students"))
					complexdf2.printSchema()
					complexdf2.show()

					val finalcomplexdf = complexdf2.select(
							col("Students"),
							struct(
									col("Permanent_address"),
									col("temporary_address")
									).alias("address"),
							col("first_name"),
							col("second_name")

							)
					finalcomplexdf.printSchema()
					finalcomplexdf.show()

					val rawdf = spark.read.format("json").option("multiLine","true").load("file:///C:/Bigdata/array1.json")
					rawdf.printSchema()
					rawdf.show()

					val intdf = rawdf.select(
							"Students",
							"address.Permanent_address",
							"address.temporary_address",
							"first_name",
							"second_name").withColumn("Students",explode(col("Students")))

					intdf.show()
					intdf.printSchema()
					
					println("=================flattendf==============")

					val flattendf2 = intdf.select(
							col("Students.user.address.Permanent_address").alias("c_Permanent_address"),
							col("Students.user.address.temporary_address").alias("c_temporary_address"),
							col("Students.user.gender"),
							col("Students.user.name.first"),
							col("Students.user.name.last"),
							col("Students.user.name.title"),
							col("Permanent_address"),
							col("temporary_address"),
							col("first_name"),
							col("second_name")
							)
					flattendf2.printSchema()
					flattendf2.show()
	}

}
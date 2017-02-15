val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val cars = sqlContext.read.json("file:///home/support1161/cars.json")
cars.show()

#Display the car names from the dataframe
cars.select("name").show()
 
#cars whose speed is greater than 300 (speed > 300)
cars.filter(cars("speed") > 300).show()

#counting the number of cars who are of the same speed.
cars.groupBy("speed").count().show()
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val crime_data = sc.textFile("file:///home/support1161/crimes.txt"); 
val schemaString = "Id ,case_no,date,block,IUCR,Primary_type,description,Loc_des,arrest,domestic,beat,district,ward,community,fbicode,XCor,YCor,year,Updated_on"
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};

val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

val crimeRdd = crime_data.map(_.split(",")).map(e => Row(e(0),e(1),e(2),e(3),e(4),e(5),e(6),e(7),e(8),e(9),e(10),e(11),e(12),e(13),e(14),e(15),e(16),e(17),e(18)))

val crimeDF = sqlContext.createDataFrame(crimeRdd, schema)

crimeDF.registerTempTable("crimes")
 
val allrecords = sqlContext.sql("select * from crimes")

allrecords.show()
 
#Find number of crimes that happened under each FBI code
val allrecords = sqlContext.sql("select fbicode,count(fbicode) as count from crimes group by fbicode")
 
#Find number of ‘NARCOTICS’ cases filed in the year 2015
val allrecords = sqlContext.sql("select count(*) as count from crimes where Primary_type ='NARCOTICS' and year = '2015' ")
allrecords.show()

#Find the number of theft related arrests that happened in each district.
val allrecords = sqlContext.sql("select district ,count(*) as count from crimes where Primary_type ='THEFT' and arrest = 'true' group by district")
allrecords.show()
 

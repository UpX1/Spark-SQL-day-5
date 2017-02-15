val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

case class Ipl(match_id: Int ,inning: Int,batting_team: String,bowling_team: String,over: Int,ball: Int,batsman: String,non_striker: String,bowler: String,is_super_over: Int,wide_runs: Int,bye_runs: Int,legbye_runs: Int,noball_runs: Int,penalty_runs: Int,batsman_runs: Int,extra_runs: Int,total_runs: Int)


val ipl=sc.textFile("hdfs:///user/support1161/ipl.txt").map(_.split("\\|")).map(i => Ipl(i(0).trim.toInt,i(1).trim.toInt, i(2),i(3),i(4).trim.toInt,i(5).trim.toInt,i(6),i(7),i(8),i(9).trim.toInt,i(10).trim.toInt,i(11).trim.toInt,i(12).trim.toInt,i(13).trim.toInt,i(14).trim.toInt,i(15).trim.toInt,i(16).trim.toInt,i(17).trim.toInt)).toDF()

ipl.registerTempTable("iplmatch")
val allrecords = sqlContext.sql("SELeCT * FROM iplmatch")

allrecords.show()
allrecords.printSchema()

#How many runs did RA Jadeja score in the match?
val allrecords = sqlContext.sql("SELeCT batsman, SUM(batsman_runs) total_runs FROM iplmatch where batsman = 'RA Jadeja' group by batsman")
allrecords.show()

#How many wides did A Nehra bowled against Chennai Super Kings in match number 8?
val allrecords = sqlContext.sql("SELeCT bowler, COUNT(wide_runs) no_of_wide_balls FROM iplmatch where bowler = 'A Nehra' and batting_team = 'Chennai Super K
ings' and wide_runs> 0 group by bowler")
allrecords.show()


#What is the final score of Mumbai Indians?
val allrecords = sqlContext.sql("SELeCT batting_team, SUM(total_runs) final_score FROM iplmatch where batting_team = 'Mumbai Indians' group by batting_team")
allrecords.show()

#Highest run scored by batsman from Royal Challengers Bangalore in a IPL series
val allrecords = sqlContext.sql("SELeCT batting_team, batsman, MAX(batsman_runs) total_runs FROM iplmatch where batting_team = 'Royal Challengers Bangalore'
 group by batting_team,batsman order by total_runs desc limit 1")
 allrecords.show()

 
#fastest 50 by a batsman in a IPL series
val allrecords = sqlContext.sql("SELeCT batsman, ball, total_runs FROM iplmatch where total_runs > 49 order by ball limit 1")
 allrecords.show()
 
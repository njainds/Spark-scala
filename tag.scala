package usertagging

object tag {
  def main(args: Array[String]) {
      import org.apache.spark._
      import org.apache.log4j.Logger
      import java.time.LocalDate;
      import com.datastax.spark.connector._;
      val conf = new SparkConf().setMaster("spark://toimisc42139:7077").setAppName("usertagging").set("spark.cassandra.connection.host", "192.168.43.100,192.168.43.101,192.168.43.102,192.168.43.103,192.168.43.104")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc);
      import sqlContext.implicits._
	  val logger = Logger.getLogger(this.getClass.getName)
 
	  //Define Dates 
val currdate2=LocalDate.now().minusDays(0).toString()
val startdate=LocalDate.now().minusDays(31).toString()
	  
val df4=sc.cassandraTable("mobiledata", "events").select("mevent","uid","timestamp","aurl").as((m:String,u:String,t:String,au:String) => (m,u,t,au)).where("appid='newspoint' and eventid >= maxTimeuuid('" + startdate + " 00:00+0000') and eventid < maxTimeuuid('" + currdate2 +" 00:00+0000')").toDF()
val names=Seq("mevent","uid","timestamp","aurl")
val df5=df4.toDF(names: _*)	
df5.registerTempTable("coke")
//sqlContext.sql("cache table coke")

val d4=sqlContext.sql("select uid,sum(case when  (col1 = 'horoscope' or col2 = 'astro') then 1 else 0 end)/count(*) as astrology,sum( case when  (col2 in ('automobile','autos')) then 1 else 0 end)/count(*) as autos,sum( case when  (col1 in ('economy','business','market') or col2 = 'business') then 1 else 0 end)/count(*) as business,sum( case when  (col2 = 'jobs') then 1 else 0 end)/count(*) as careers,sum( case when  (col1 ='article' or col2 in ('articleshow','story')) then 1 else 0 end)/count(*) as editorial,sum( case when  (col1 = 'education' ) then 1 else 0 end)/count(*) as education,sum( case when  (col1 in ('entertainment','celebs','jokes','photo','bollywood','picture','sex','sunny-leones','bigg-boss') or col2 in ('gossip','celeb-themes','bollywood','foreign-models','actress','foreign-shows','entertainment','jokes','shows')) then 1 else 0 end)/count(*) as entertainment,sum( case when  (col2 in ('health','fitness')) then 1 else 0 end)/count(*) as health,sum( case when  (col1 in ('world','world-news','international') or col2 in ('world','america','international')) then 1 else 0 end)/count(*) as international,sum( case when  (col1 in ('lifestyle','social','fashion','recipes','beauty') or col2 in ('relationship','ifestyle','fashion','beauty')) then 1 else 0 end)/count(*) as lifestyle,sum( case when  (col1 in ('movie','cinema') or col2 in ('movie','cinema')) then 1 else 0 end)/count(*) as movie_reviews,sum( case when  (col1 in ('politics','poll','budget','election') or col2 in ('politics','poll','budget','election')) then 1 else 0 end)/count(*) as politics,sum( case when  (col1 in ('district','state','bengali','maharashtra','uttar-pradesh','city','urdu','nepali','kerala','kannada','punjab','gujarat','delhi-and-ncr','punjab','state-news','kolkata','mumbai-news','karnataka','bengalurucity','south-bengal','kannada','ahmedabad','telugu','mysuru') or col2 in ('district','state','bengali','maharashtra','uttar-pradesh','city','urdu','nepali','kerala','kannada','punjab','gujarat','delhi-and-ncr','punjab','state-news','kolkata','mumbai-news','karnataka','bengalurucity','south-bengal','kannada','ahmedabad','telugu','mysuru')) then 1 else 0 end)/count(*) as regional,sum( case when  (col1 in ('religion','spiritual') ) then 1 else 0 end)/count(*) as spirituaity,sum( case when  (col1 in ('sports','cricket') or col2 in ('sports','cricket')) then 1 else 0 end)/count(*) as sports,sum( case when  (col1 in ('tech','technology','science') or col2 in ('tech','technology','science')) then 1 else 0 end)/count(*) as tech,sum( case when  (col1 in ('india-news','tv-news','nation','national','latest-news') or col2 in ('india-news','tv-news','nation','national','latest-news')) then 1 else 0 end)/count(*) as top_news,count(*) as tot_readevent from (select uid,lower(trim(substr(aurl,locate('/',aurl,8)+1,(locate('/',aurl,(locate('/',aurl,8)+1))-locate('/',aurl,8)-1)))) as col1,lower(trim(substr(aurl,locate('/',aurl,locate('/',aurl,locate('/',aurl,locate('/',aurl,8))+1))+1,(locate('/',aurl,locate('/',aurl,locate('/',aurl,locate('/',aurl,locate('/',aurl,8))+1))+1)-locate('/',aurl,locate('/',aurl,locate('/',aurl,locate('/',aurl,8))+1)))-1))) as col2 from coke where mevent='read') group by uid")
d4.registerTempTable("table")
sqlContext.sql("cache table table")
val d5=sqlContext.sql("select uid,pref_catg,value from (select uid,astrology value , 'astrology' pref_catg from table union all select uid,autos value , 'autos' pref_catg from table union all select uid,business value , 'business' pref_catg from table union all select uid,careers value , 'careers' pref_catg from table union all select uid,editorial value , 'editorial' pref_catg from table union all select uid,education value , 'education' pref_catg from table union all select uid,entertainment value , 'entertainment' pref_catg from table union all select uid,health value , 'health' pref_catg from table union all select uid,international value , 'international' pref_catg from table union all select uid,lifestyle value , 'lifestyle' pref_catg from table union all select uid,movie_reviews value , 'movie_reviews' pref_catg from table union all select uid,politics value , 'politics' pref_catg from table union all select uid,regional value , 'regional' pref_catg from table union all select uid,spirituaity value , 'spirituaity' pref_catg from table union all select uid,sports value , 'sports' pref_catg from table union all select uid,tech value , 'tech' pref_catg from table union all select uid,top_news value , 'top_news' pref_catg from table)")
d5.registerTempTable("d5")
val d6=sqlContext.sql("select month(to_date(now())) as month,year(to_date(now())) as year,d5.uid,d5.pref_catg,d5.value,table.tot_readevent from d5 left join table on d5.uid=table.uid")
d6.registerTempTable("d6")
val d7=sqlContext.sql("select * from d6 where ((value>0.5 and tot_readevent>4) or (value>0.75 and tot_readevent>=3)) and pref_catg in ('entertainment','lifestyle','sports','business','health','tech')")
import com.datastax.driver.core.utils.UUIDs;
import org.apache.spark.sql.functions.udf
val generateUUID = udf(() => UUIDs.timeBased().toString)
val d8=d7.withColumn("createdtime", generateUUID())
d8.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "usercategorytagging", "keyspace" -> "mobiledata")).mode("append").save()
}
}
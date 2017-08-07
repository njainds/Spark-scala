package CokeSegment

object CokeSegmentProfile {
  def main(args: Array[String]) {
      import org.apache.spark._
      import org.apache.log4j.Logger
      import java.time.LocalDate;
      import com.datastax.spark.connector._;
      val conf = new SparkConf().setMaster("spark://dsciapp43104:7077").setAppName("CokeSegmentProfile").set("spark.cassandra.connection.host", "192.168.43.100,192.168.43.101,192.168.43.102,192.168.43.103,192.168.43.104")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc);
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._
      import scala.concurrent.duration._
      import java.io._;
      import scala.util.parsing.json._
	  import org.apache.http.client.methods.HttpPost
	  import org.apache.http.entity.StringEntity
	  import org.apache.http.impl.client.DefaultHttpClient
	  import com.google.gson.Gson
val logger = Logger.getLogger(this.getClass.getName)
val deadline = 72000.seconds.fromNow

//Read profile data
val df1a=sc.cassandraTable("mobiledata", "profiles").select("uid","uasource","aversion","localecity","osversion","scresolution","dtype","idate").as((m:String,u:String,t:String,ma:String,ua:String,ta:String,mb:String,ub:String) => (m,u,t,ma,ua,ta,mb,ub)).where("appid='newspoint'").toDF()
val names=Seq("uid","asource","aversion","localecity","osversion","scresolution","dtype","idate")
val df2a=df1a.toDF(names: _*)
df2a.registerTempTable("table1a")

//Read preferences data
val df1b=sc.cassandraTable("mobiledata", "preferences").select("uid","channelids","languages").as((m:String,u:String,t:String) => (m,u,t)).where("appid='newspoint'").toDF()
val names1=Seq("uid","channelids","languages")
val df2b=df1b.toDF(names1: _*)
df2b.registerTempTable("table1b")

// Read device price
val deviceprice=sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "deviceprice_27apr", "keyspace" -> "nj_test")).load()
deviceprice.registerTempTable("deviceprice")

val df1=sqlContext.sql("select a.uid,a.asource,a.aversion,a.localecity,a.osversion,a.scresolution,a.dtype,a.idate,a.dtype2,b.price,b.price_range,table1b.channelids,table1b.languages from ((select *,(case when instr(substr(upper(trim(dtype)),locate('/',upper(trim(dtype)))+1),substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1))>0 then substr(upper(trim(dtype)),locate('/',upper(trim(dtype)))+1) else concat(substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1),' ', substr(upper(trim(dtype)),locate('/',upper(trim(dtype)))+1)) end) as dtype2 from table1a) a left join table1b on a.uid=table1b.uid left join deviceprice b on a.dtype2=b.id)")
df1.registerTempTable("table1")
sqlContext.sql("cache table table1")
logger.info("################Table1 cached################")

//Loop starts
while(deadline.hasTimeLeft) {

//Read API and get value of segment and jobid
val readjob=scala.io.Source.fromURL("http://192.168.38.178/job/v2/filters?app=newspoint&jobTypes=EXTERNAL_SEGMENT_X_RAY&statusValues=SUBMITTED").mkString

// check if any job has been submitted or not
if(readjob.indexOfSlice("\"jobStatus\":\"SUBMITTED\",") >= 0 & readjob.indexOfSlice("\"app\":\"newspoint\"") >= 0) {

// Parsing the jobid and segment of the jobs submitted
val jsonString=readjob.stripMargin
class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

object M extends CC[Map[String, Any]]
object L extends CC[List[Any]]
object S extends CC[Double]
object D extends CC[String]
object B extends CC[Map[String, String]]
object BB extends CC[String]

val result = for {
    Some(M(map)) <- List(JSON.parseFull(jsonString))
    L(response) = map("response")
    M(response1) <- response
    S(jobid) = response1("jobId")
    D(jobstatus) = response1("jobStatus")
    B(segments) = response1("inputData")
	BB(segment)=segments("segmentid")
	BB(apps)=segments("app")
	BB(segment_compare)=segments("segmentid_compare")
} yield {
    (jobid.toInt, jobstatus,segment,apps,segment_compare)
}

//Reading jobid and segment
for (i <- 1 to result.length) {
if(result(i-1)._4 =="newspoint") {
val jobid=result(i-1)._1
val segment=result(i-1)._3
val segment_compare=result(i-1)._5
logger.info("#############Segmentpicked:"+ segment +"####Jobidpicked: " + jobid + "############")

//Writing the status as "executing" to the api for the job picked
val stat="{\"id\":" + jobid + "," + "\"status\":\"EXECUTING\","  + "\"startTime\":" + System.currentTimeMillis() + "," + "\"response\":{\"result\":\"null\"}}"

// create an HttpPost object
val post = new HttpPost("http://192.168.38.178/job/persistJobResult")
post.setHeader("Content-type", "application/json")
post.setEntity(new StringEntity(stat))
val response = (new DefaultHttpClient).execute(post)

// Read API to get uid from segment ID
val str1=scala.io.Source.fromURL("http://192.168.34.105:8080/coke/segment/users/uthintrnl?app=Appid&id="+ segment).mkString
val str=str1.replaceAll("\"","")
val rdd=sc.parallelize((str.substring(str.indexOf(":[")+2,str.indexOf("]}"))).split(","))
val df=rdd.toDF()
val uid=df.toDF(Seq("uid"):_*)
uid.registerTempTable("uidtable")

val df2=sqlContext.sql("select table1.* from (uidtable left join table1 on uidtable.uid=table1.uid)")
df2.registerTempTable("table2")
sqlContext.sql("cache table table2")

// Read API to get uid from segment ID compare
val str1c=scala.io.Source.fromURL("http://192.168.34.105:8080/coke/segment/users/uthintrnl?app=Appid&id="+ segment_compare).mkString
val strc=str1c.replaceAll("\"","")
val rddc=sc.parallelize((strc.substring(strc.indexOf(":[")+2,strc.indexOf("]}"))).split(","))
val dfc=rddc.toDF()
val uidc=dfc.toDF(Seq("uid"):_*)
uidc.registerTempTable("uidtablec")

val df2c=sqlContext.sql("select table1.* from (uidtablec left join table1 on uidtablec.uid=table1.uid)")
df2c.registerTempTable("table2c")
sqlContext.sql("cache table table2c")

val stat1="{\"id\":" + jobid + "," + "\"status\":\"EXECUTING\","  + "\"completionPercentage\": 20,"  + "\"response\":{\"result\":\"null\"}}"
val post1 = new HttpPost("http://192.168.38.178/job/persistJobResult")
post1.setHeader("Content-type", "application/json")
post1.setEntity(new StringEntity(stat1))
val response1 = (new DefaultHttpClient).execute(post1)


val df3=df2.select(expr("(split(channelids, ','))[0]").cast("string").as("col1"),expr("(split(channelids, ','))[1]").cast("string").as("col2"),expr("(split(channelids, ','))[2]").cast("string").as("col3"),expr("(split(channelids, ','))[3]").cast("string").as("col4"),expr("(split(channelids, ','))[4]").cast("string").as("col5"),expr("(split(channelids, ','))[5]").cast("string").as("col6"),expr("(split(channelids, ','))[6]").cast("string").as("col7"),expr("(split(channelids, ','))[7]").cast("string").as("col8"),expr("(split(channelids, ','))[8]").cast("string").as("col9"),expr("(split(channelids, ','))[9]").cast("string").as("col10"),expr("(split(channelids, ','))[10]").cast("string").as("col11"),expr("(split(channelids, ','))[11]").cast("string").as("col12"),expr("(split(channelids, ','))[12]").cast("string").as("col13"),expr("(split(channelids, ','))[13]").cast("string").as("col14"),expr("(split(channelids, ','))[14]").cast("string").as("col15"),expr("(split(channelids, ','))[15]").cast("string").as("col16"),expr("(split(channelids, ','))[16]").cast("string").as("col17"),expr("(split(channelids, ','))[17]").cast("string").as("col18"),expr("(split(channelids, ','))[18]").cast("string").as("col19"),expr("(split(channelids, ','))[19]").cast("string").as("col20"),expr("(split(channelids, ','))[20]").cast("string").as("col21"),expr("(split(channelids, ','))[21]").cast("string").as("col22"),expr("(split(channelids, ','))[22]").cast("string").as("col23"),expr("(split(channelids, ','))[23]").cast("string").as("col24"),expr("(split(channelids, ','))[24]").cast("string").as("col25"),expr("(split(channelids, ','))[25]").cast("string").as("col26"),expr("(split(channelids, ','))[26]").cast("string").as("col27"),expr("(split(channelids, ','))[27]").cast("string").as("col28"),expr("(split(channelids, ','))[28]").cast("string").as("col29"),expr("(split(channelids, ','))[29]").cast("string").as("col30"), $"uid")
df3.registerTempTable("table3")
sqlContext.sql("cache table table3")
val df4=sqlContext.sql("select uid,col1 value from table3 union all select uid,col2 value from table3 union all select uid,col3 value from table3 union all select uid,col4 value from table3 union all select uid,col5 value from table3 union all select uid,col6 value from table3 union all select uid,col7 value from table3 union all select uid,col8 value from table3 union all select uid,col9 value from table3 union all select uid,col10 value from table3 union all select uid,col11 value from table3 union all select uid,col12 value from table3 union all select uid,col13 value from table3 union all select uid,col14 value from table3 union all select uid,col15 value from table3 union all select uid,col16 value from table3 union all select uid,col17 value from table3 union all select uid,col18 value from table3 union all select uid,col19 value from table3 union all select uid,col20 value from table3 union all select uid,col21 value from table3 union all select uid,col22 value from table3 union all select uid,col23 value from table3 union all select uid,col24 value from table3 union all select uid,col25 value from table3 union all select uid,col26 value from table3 union all select uid,col27 value from table3 union all select uid,col28 value from table3 union all select uid,col29 value from table3 union all select uid,col30 value from table3")
df4.registerTempTable("table4")

val df3c=df2c.select(expr("(split(channelids, ','))[0]").cast("string").as("col1"),expr("(split(channelids, ','))[1]").cast("string").as("col2"),expr("(split(channelids, ','))[2]").cast("string").as("col3"),expr("(split(channelids, ','))[3]").cast("string").as("col4"),expr("(split(channelids, ','))[4]").cast("string").as("col5"),expr("(split(channelids, ','))[5]").cast("string").as("col6"),expr("(split(channelids, ','))[6]").cast("string").as("col7"),expr("(split(channelids, ','))[7]").cast("string").as("col8"),expr("(split(channelids, ','))[8]").cast("string").as("col9"),expr("(split(channelids, ','))[9]").cast("string").as("col10"),expr("(split(channelids, ','))[10]").cast("string").as("col11"),expr("(split(channelids, ','))[11]").cast("string").as("col12"),expr("(split(channelids, ','))[12]").cast("string").as("col13"),expr("(split(channelids, ','))[13]").cast("string").as("col14"),expr("(split(channelids, ','))[14]").cast("string").as("col15"),expr("(split(channelids, ','))[15]").cast("string").as("col16"),expr("(split(channelids, ','))[16]").cast("string").as("col17"),expr("(split(channelids, ','))[17]").cast("string").as("col18"),expr("(split(channelids, ','))[18]").cast("string").as("col19"),expr("(split(channelids, ','))[19]").cast("string").as("col20"),expr("(split(channelids, ','))[20]").cast("string").as("col21"),expr("(split(channelids, ','))[21]").cast("string").as("col22"),expr("(split(channelids, ','))[22]").cast("string").as("col23"),expr("(split(channelids, ','))[23]").cast("string").as("col24"),expr("(split(channelids, ','))[24]").cast("string").as("col25"),expr("(split(channelids, ','))[25]").cast("string").as("col26"),expr("(split(channelids, ','))[26]").cast("string").as("col27"),expr("(split(channelids, ','))[27]").cast("string").as("col28"),expr("(split(channelids, ','))[28]").cast("string").as("col29"),expr("(split(channelids, ','))[29]").cast("string").as("col30"), $"uid")
df3c.registerTempTable("table3c")
sqlContext.sql("cache table table3c")
val df4c=sqlContext.sql("select uid,col1 value from table3c union all select uid,col2 value from table3c union all select uid,col3 value from table3c union all select uid,col4 value from table3c union all select uid,col5 value from table3c union all select uid,col6 value from table3c union all select uid,col7 value from table3c union all select uid,col8 value from table3c union all select uid,col9 value from table3c union all select uid,col10 value from table3c union all select uid,col11 value from table3c union all select uid,col12 value from table3c union all select uid,col13 value from table3c union all select uid,col14 value from table3c union all select uid,col15 value from table3c union all select uid,col16 value from table3c union all select uid,col17 value from table3c union all select uid,col18 value from table3c union all select uid,col19 value from table3c union all select uid,col20 value from table3c union all select uid,col21 value from table3c union all select uid,col22 value from table3c union all select uid,col23 value from table3c union all select uid,col24 value from table3c union all select uid,col25 value from table3c union all select uid,col26 value from table3c union all select uid,col27 value from table3c union all select uid,col28 value from table3c union all select uid,col29 value from table3c union all select uid,col30 value from table3c")
df4c.registerTempTable("table4c")

val stat2="{\"id\":" + jobid + "," + "\"status\":\"EXECUTING\","  + "\"completionPercentage\": 40,"  + "\"response\":{\"result\":\"null\"}}"
val post2 = new HttpPost("http://192.168.38.178/job/persistJobResult")
post2.setHeader("Content-type", "application/json")
post2.setEntity(new StringEntity(stat2))
val response2 = (new DefaultHttpClient).execute(post2)

val a=uid.count().toString()
val seg_df=sqlContext.sql("(select 'asource' as feature,asource as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by asource order by perc_users desc limit 10) union all (select 'aversion' as feature,aversion as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by aversion order by perc_users desc limit 10) union all (select 'localecity' as feature,localecity as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by localecity order by perc_users desc limit 10) union all (select 'osversion' as feature,osversion as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by osversion order by perc_users desc limit 10) union all (select 'scresolution' as feature,scresolution as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by scresolution order by perc_users desc limit 10) union all (select 'install_day' as feature,date_format(CAST(CAST(concat(substring(idate, 7, 4), '-', substring(idate, 4, 2), '-', substring(idate, 1, 2)) AS DATE) AS TIMESTAMP), 'EEEE') as feature_values ,count(distinct uid)/"+ a +" as perc_users from table2 group by date_format(CAST(CAST(concat(substring(idate, 7, 4), '-', substring(idate, 4, 2), '-', substring(idate, 1, 2)) AS DATE) AS TIMESTAMP), 'EEEE') order by perc_users desc limit 10) union all (select 'languages' as feature,languages as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by languages order by perc_users desc limit 10) union all (select 'Device_brand' as feature,substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1) as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1) order by perc_users desc limit 10) union all (select 'device_price' as feature,price_range as feature_values,count(distinct uid)/"+ a +" as perc_users from table2 group by price_range order by perc_users desc limit 10) union all (select 'Publication' as feature,value as feature_values,count(distinct uid)/"+ a +" as perc_users from table4 where value is not null group by value order by perc_users desc limit 10)")
val seg_df2 = seg_df.na.fill("Not_Available",Seq("feature_values"))
seg_df2.registerTempTable("table5")
sqlContext.sql("cache table table5")

val ac=uidc.count().toString()
val seg_dfc=sqlContext.sql("(select 'asource' as feature,asource as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by asource ) union all (select 'aversion' as feature,aversion as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by aversion ) union all (select 'localecity' as feature,localecity as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by localecity ) union all (select 'osversion' as feature,osversion as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by osversion ) union all (select 'scresolution' as feature,scresolution as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by scresolution ) union all (select 'install_day' as feature,date_format(CAST(CAST(concat(substring(idate, 7, 4), '-', substring(idate, 4, 2), '-', substring(idate, 1, 2)) AS DATE) AS TIMESTAMP), 'EEEE') as feature_values ,count(distinct uid)/"+ ac +" as perc_users from table2c group by date_format(CAST(CAST(concat(substring(idate, 7, 4), '-', substring(idate, 4, 2), '-', substring(idate, 1, 2)) AS DATE) AS TIMESTAMP), 'EEEE') ) union all (select 'languages' as feature,languages as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by languages ) union all (select 'Device_brand' as feature,substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1) as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by substr(upper(trim(dtype)),1,locate('/',upper(trim(dtype)))-1) ) union all (select 'device_price' as feature,price_range as feature_values,count(distinct uid)/"+ ac +" as perc_users from table2c group by price_range ) union all (select 'Publication' as feature,value as feature_values,count(distinct uid)/"+ ac +" as perc_users from table4c where value is not null group by value )")
val seg_df2c = seg_dfc.na.fill("Not_Available",Seq("feature_values"))
seg_df2c.registerTempTable("table5c")
sqlContext.sql("cache table table5c")

val stat3="{\"id\":" + jobid + "," + "\"status\":\"EXECUTING\","  + "\"completionPercentage\": 60,"  + "\"response\":{\"result\":\"null\"}}"
val post3 = new HttpPost("http://192.168.38.178/job/persistJobResult")
post3.setHeader("Content-type", "application/json")
post3.setEntity(new StringEntity(stat3))
val response3 = (new DefaultHttpClient).execute(post3)

logger.info("################table5 cached for jobid:" + jobid + "################")

def getjson(a:String): String = {
return(sqlContext.sql("select * from table5 where feature='"+ a +"'").groupBy("feature").pivot("feature_values").sum("perc_users").drop("feature").toJSON.collect().mkString)
}

val all1=sqlContext.sql("select a.feature,a.feature_values,b.perc_users from (table5 a left join table5c b on a.feature=b.feature and a.feature_values=b.feature_values)")
val all2 = all1.na.fill(0,Seq("perc_users"))
all2.registerTempTable("All_table5")
sqlContext.sql("cache table All_table5")

def getjson_all(a:String): String = {
return(sqlContext.sql("select * from All_table5 where feature='"+ a +"'").groupBy("feature").pivot("feature_values").sum("perc_users").drop("feature").toJSON.collect().mkString)
}

import java.lang.reflect.{Type, ParameterizedType}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.`type`.TypeReference;

val mapper = new ObjectMapper()
mapper.registerModule(DefaultScalaModule)

def serialize(value: Any): String = {
  import java.io.StringWriter
  val writer = new StringWriter()
  mapper.writeValue(writer, value)
  writer.toString
}

val stat4="{\"id\":" + jobid + "," + "\"status\":\"EXECUTING\","  + "\"completionPercentage\": 80,"  + "\"response\":{\"result\":\"null\"}}"
val post4 = new HttpPost("http://192.168.38.178/job/persistJobResult")
post4.setHeader("Content-type", "application/json")
post4.setEntity(new StringEntity(stat4))
val response4 = (new DefaultHttpClient).execute(post4)

// Ref: http://stackoverflow.com/questions/12591457/scala-2-10-json-serialization-and-deserialization
val out="{\"id\":" + jobid + "," + "\"status\":\"SUCCESS\"," + "\"completionPercentage\": 100," + "\"endTime\":" + System.currentTimeMillis() + "," + "\"response\":" + "{\"result\":" + serialize("[" + "{\"id\":\"asource\",\"name\":\"Acquisition Source\",\"display\":\"bar\",\"data\":" + getjson("asource") + "}," + "{\"id\":\"aversion\",\"name\":\"App Version\",\"display\":\"bar\",\"data\":" + getjson("aversion") + "}," + "{\"id\":\"localecity\",\"name\":\"City\",\"display\":\"bar\",\"data\":" + getjson("localecity") + "}," + "{\"id\":\"osversion\",\"name\":\"OS Version\",\"display\":\"bar\",\"data\":" + getjson("osversion") + "}," + "{\"id\":\"scresolution\",\"name\":\"Screen Resolution\",\"display\":\"bar\",\"data\":" + getjson("scresolution") + "}," + "{\"id\":\"install_day\",\"name\":\"Day of Install\",\"display\":\"bar\",\"data\":" + getjson("install_day") + "}," +"{\"id\":\"languages\",\"name\":\"Language\",\"display\":\"bar\",\"data\":" + getjson("languages") + "},"  + "{\"id\":\"Device_brand\",\"name\":\"Device Brand\",\"display\":\"bar\",\"data\":" + getjson("Device_brand") + "}," + "{\"id\":\"device_price\",\"name\":\"Device Price\",\"display\":\"bar\",\"data\":" + getjson("device_price") + "}," + "{\"id\":\"Publication\",\"name\":\"Publication\",\"display\":\"bar\",\"data\":" + getjson("Publication") + "}" + "]") + "," + "\"result_compare\":" + serialize("[" + "{\"id\":\"asource\",\"name\":\"Acquisition Source\",\"display\":\"bar\",\"data\":" + getjson_all("asource") + "}," + "{\"id\":\"aversion\",\"name\":\"App Version\",\"display\":\"bar\",\"data\":" + getjson_all("aversion") + "}," + "{\"id\":\"localecity\",\"name\":\"City\",\"display\":\"bar\",\"data\":" + getjson_all("localecity") + "}," + "{\"id\":\"osversion\",\"name\":\"OS Version\",\"display\":\"bar\",\"data\":" + getjson_all("osversion") + "}," + "{\"id\":\"scresolution\",\"name\":\"Screen Resolution\",\"display\":\"bar\",\"data\":" + getjson_all("scresolution") + "}," + "{\"id\":\"install_day\",\"name\":\"Day of Install\",\"display\":\"bar\",\"data\":" + getjson_all("install_day") + "}," +"{\"id\":\"languages\",\"name\":\"Language\",\"display\":\"bar\",\"data\":" + getjson_all("languages") + "},"  + "{\"id\":\"Device_brand\",\"name\":\"Device Brand\",\"display\":\"bar\",\"data\":" + getjson_all("Device_brand") + "}," + "{\"id\":\"device_price\",\"name\":\"Device Price\",\"display\":\"bar\",\"data\":" + getjson_all("device_price") + "}," + "{\"id\":\"Publication\",\"name\":\"Publication\",\"display\":\"bar\",\"data\":" + getjson_all("Publication") + "}" + "]") +"}}"

//post results as json along with other parameters: segment, jobid, updatedtime
val postf = new HttpPost("http://192.168.38.178/job/persistJobResult")
postf.setHeader("Content-type", "application/json")
postf.setEntity(new StringEntity(out))
val responsef = (new DefaultHttpClient).execute(postf)
}
}
}
}
sc.stop()

}
}
package coke
object CokeRetention {
   def main(args: Array[String]) {
      import org.apache.spark._
      import org.apache.log4j.Logger
      import java.time.LocalDate;
      import com.datastax.spark.connector._;
      val conf = new SparkConf().setMaster("spark://toimisc42139:7077").setAppName("CokeRetention").set("spark.cassandra.connection.host", "192.168.43.100,192.168.43.101,192.168.43.102,192.168.43.103,192.168.43.104")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc);
      import sqlContext.implicits._
	  val logger = Logger.getLogger(this.getClass.getName)

      // Define dynamics variables
      val currdate=LocalDate.now().minusDays(1).toString()
      val startdate=LocalDate.now().minusDays(92).toString()
      val installenddate=LocalDate.now().minusDays(1).toString()
      logger.info("################dates defined################")
	  val applist = List("newspoint");
	  logger.info("#################APPID IS NEWSPOINT#################")
	  for( appid <- applist ){
	     // fetching the required event data
         val df1=sc.cassandraTable("mobiledata", "events").select("mevent","uid","timestamp").as((m:String,u:String,t:String) => (m,u,t)).where("appid='" + appid + "' and eventid > maxTimeuuid('" + startdate + " 00:00+0000') and eventid < maxTimeuuid('" + currdate +" 00:00+0000')").toDF()
         val names=Seq("mevent","uid","timestamp")
         val df2=df1.toDF(names: _*)
         df2.registerTempTable("coke")
         sqlContext.sql("cache table coke")
		 
		 val segmentlist = List("srLb2SiM","GKwwpGTt");
         for (segment <- segmentlist){
		     // Fetching the uids correspondings to the segmentid
             val str1=scala.io.Source.fromURL("http://192.168.34.105:8080/coke/segment/users?app=Appid&id="+ segment).mkString
             val str=str1.replaceAll("\"","")
             val rdd=sc.parallelize((str.substring(str.indexOf(":[")+2,str.indexOf("]}"))).split(","))
             val df=rdd.toDF()
             val uid=df.toDF(Seq("uid"):_*)
             uid.registerTempTable("uidtable")
		     
			 // calculating the weekly retention numbers
             val df3=sqlContext.sql("select ('" + startdate + "') as installfromdate, ('" + installenddate + "') as installtodate,NOW() as lastupdatedtime,sum(case when date_add(idate,7) <= '" + currdate + "' then w2 else 0 end)/sum(case when date_add(idate,7) <= '" + currdate + "' then users else 0 end) as w2,sum(case when date_add(idate,14) <= '" + currdate + "' then w3 else 0 end)/sum(case when date_add(idate,14) <= '" + currdate + "' then users else 0 end) as w3,sum(case when date_add(idate,21) <= '" + currdate + "' then w4 else 0 end)/sum(case when date_add(idate,21) <= '" + currdate + "' then users else 0 end) as w4,sum(case when date_add(idate,30) <= '" + currdate + "' then m2 else 0 end)/sum(case when date_add(idate,30) <= '" + currdate + "' then users else 0 end) as m2,sum(case when date_add(idate,60) <= '" + currdate + "' then m3 else 0 end)/sum(case when date_add(idate,60) <= '" + currdate + "' then users else 0 end) as m3,sum(case when date_add(idate,90) <= '" + currdate + "' then m4 else 0 end)/sum(case when date_add(idate,90) <= '" + currdate + "' then users else 0 end) as m4 from (select idate,count(distinct uid) as users,sum(case when w2=0 then 1 else 0 end) as w2,sum(case when w3=0 then 1 else 0 end) as w3,sum(case when w4=0 then 1 else 0 end) as w4,sum(case when m2=0 then 1 else 0 end) as m2,sum(case when m3=0 then 1 else 0 end) as m3,sum(case when m4=0 then 1 else 0 end) as m4 from (select a.uid,idate,max(case when maxtime < date_add(idate,7) or (mevent='install' and timedate< date_add(idate,7) and timedate>idate) then 1 else 0 end) as w2,max(case when maxtime < date_add(idate,14) or (mevent='install' and timedate<date_add(idate,14) and timedate>idate) then 1 else 0 end) as w3, max(case when maxtime < date_add(idate,21) or (mevent='install' and timedate< date_add(idate,21) and timedate>idate) then 1 else 0 end) as w4,max(case when maxtime < date_add(idate,30) or (mevent='install' and timedate< date_add(idate,30) and timedate>idate) then 1 else 0 end) as m2,max(case when maxtime < date_add(idate,60) or (mevent='install' and timedate<date_add(idate,60) and timedate>idate) then 1 else 0 end) as m3, max(case when maxtime < date_add(idate,90) or (mevent='install' and timedate< date_add(idate,90) and timedate>idate) then 1 else 0 end) as m4 from ((select distinct uid from uidtable) aa join (select distinct uid,cast(substr(timestamp,1,10) as date) as idate from  coke where mevent='install' and cast(substr(timestamp,1,10) as date) >= '" + startdate + "' and cast(substr(timestamp,1,10) as date) <= '" + installenddate + "') a on aa.uid=a.uid left join (select uid,max(cast(substr(timestamp,1,10) as date)) as maxtime from coke group by uid) b on aa.uid=b.uid left join (select distinct uid,cast(substr(timestamp,1,10) as date) as timedate,mevent from coke) c on aa.uid=c.uid) group by  a.uid,idate) group by idate)")
             import org.apache.spark.sql.functions.lit
             val df4=df3.withColumn("segment",lit(segment))
			 
			 //writing to cassandra
             df4.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "sample", "keyspace" -> "nj_test")).mode("append").save()
			 logger.info("#########################FILE written in cassandra#####################################")
             }
		 sqlContext.uncacheTable("coke")	 
	    }
		sc.stop()
      }
   }
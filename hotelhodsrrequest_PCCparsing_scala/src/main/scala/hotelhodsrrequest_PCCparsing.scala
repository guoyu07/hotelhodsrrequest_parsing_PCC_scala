/**
  * Created by SG0952655 on 10/24/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import org.apache.hadoop.io.compress.GzipCodec

object hotelhodsrrequest_PCCparsing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hotelhodsrrequest_PCCparsing")

    /**The following solutions use pyspark, but I assume the code in Scala would be similar.
    *First option is to set the following when you initialise your SparkConf:
    *conf.set("spark.hadoop.mapred.output.compress", "true")
    *conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    *conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    *conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    */


    /**Spark: writing DataFrame as compressed JSON [Resolved]
      * Second option, if you want to compress only selected files within your context. Lets say "df" is your dataframe and filename your destination:
      * df_rdd = self.df.toJSON()
      * df_rdd.saveAsTextFile(filename,compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
      */

    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2016/month=01/day=*/*.gz")

    val PCClist1 = Array("01N9", "Y1DH")
    val PCClist2 = Array("Y6BA", "R37F")
    val PCClist3 = Array("S1I9", "9WQF")
    val PCClist4 = Array("NR90", "X9TH")
   
    val PCClist = Array.concat(PCClist1, PCClist2, PCClist3, PCClist4)
      
    val PCCArray = textFile.map{line =>  line.split('|')}.filter{line => ((line{7}.nonEmpty) && (PCClist contains line{4}))}
     
    val MapedPCCArray = PCCArray.map(line =>  (line{4}+" "+line{6}+" "+ (line{0}.split(" "){0}).split("-"){1}, 1))
     
    val ReducePCCArray = MapedPCCArray.reduceByKey(_+_)

    val Record_count = ReducePCCArray.count()

    val Record_count_RDD = sc.parallelize(Array(Record_count))

    Record_count_RDD.saveAsTextFile("/user/sg952655/Record_count_RDD/")

    ReducePCCArray.saveAsTextFile("/user/sg952655/ReducePCCArray/", classOf[GzipCodec])

    /**
      * import org.apache.hadoop.io.compress.GzipCodec
      * combPrdGrp3.repartition(10).saveAsTextFile("/user/sg952655/ReducePCCArray/", classOf[GzipCodec])
      */
  }

}

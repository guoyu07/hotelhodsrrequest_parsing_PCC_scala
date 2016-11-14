/**
  * Created by SG0952655 on 10/25/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import org.apache.hadoop.io.compress.GzipCodec



object hotelhodsrrequest_PCCparsing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hotelhodsrrequest_PCCparsing")

    val sc = new SparkContext(conf)

    val textFile1 = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2015/month=10/day=*/*.gz")
    val textFile2 = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2015/month=11/day=*/*.gz")
    val textFile3 = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2015/month=12/day=*/*.gz")
    val textFile_rdds = Seq(textFile1, textFile2, textFile3)
    val textFile = sc.union(textFile_rdds)

    val PCClist1 = Array1.Array_1()
    val PCClist2 = Array1.Array_2()
    val PCClist3 = Array2.Array_3()
    val PCClist4 = Array2.Array_4()
    val PCClist5 = Array3.Array_5()
    val PCClist6 = Array3.Array_6()
    val PCClist7 = Array4.Array_7()
    val PCClist8 = Array4.Array_8()
    val PCClist9 = Array5.Array_9()
    val PCClist10 = Array5.Array_10()
    val PCClist11 = Array6.Array_11()
    val PCClist12 = Array6.Array_12()
    val PCClist13 = Array7.Array_13()
    val PCClist14 = Array7.Array_14()
    val PCClist15 = Array8.Array_15()
    val PCClist16 = Array8.Array_16()
    val PCClist17 = Array9.Array_17()
    val PCClist18 = Array9.Array_18()
    val PCClist19 = Array10.Array_19()
    val PCClist20 = Array10.Array_20()


    val PCClist = Array.concat(PCClist1, PCClist2, PCClist3, PCClist4, PCClist5, PCClist6, PCClist7, PCClist8, PCClist9, PCClist10, PCClist11, PCClist12, PCClist13, PCClist14, PCClist15,PCClist16, PCClist17,PCClist18,PCClist19, PCClist20)

    val PCCArray = textFile.map{line =>  line.split('|')}.filter{line => ((line{7}.nonEmpty) && (PCClist contains line{4}))}

    val MapedPCCArray = PCCArray.map(line =>  (line{4}+" "+line{6}+" "+(line{0}.split(" "){0}).split("-"){1}+" "+(line{0}.split(" "){0}).split("-"){0}, 1))

    val ReducePCCArray = MapedPCCArray.reduceByKey(_+_).map( line => (line._1.split(" "){0}+"|"+line._1.split(" "){1}+"|"+line._1.split(" "){3}+"|"+line._1.split(" "){2}+"|"+line._2))

    val Record_count = ReducePCCArray.count()

    val Record_count_RDD = sc.parallelize(Array(Record_count))

    Record_count_RDD.saveAsTextFile("/user/sg952655/Record_count_RDD_2015/")

    ReducePCCArray.saveAsTextFile("/user/sg952655/ReducePCCArray_2015/", classOf[GzipCodec])


  }
}

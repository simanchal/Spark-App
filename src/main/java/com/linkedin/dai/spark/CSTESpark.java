package com.linkedin.dai.spark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;


//spark-submit --class com.linkedin.dai.spark.CSTESpark --master yarn-cluster cste.jar
//spark-submit --class com.linkedin.dai.spark.CSTESpark --master yarn-cluster --driver-memory 4g --executor-memory 3g cste.jar
//spark-submit --class com.linkedin.dai.spark.CSTESpark --master yarn-client --num-executors 10 --executor-cores 10 --driver-memory 10G --executor-memory 10G cste.jar
public class CSTESpark {

  /** Spam type and order mapping map */
  private static final Map<String, Integer> SPAM_ORDER_MAP = new HashMap<String, Integer>(20);

  static {
    SPAM_ORDER_MAP.put("MONEY_FRAUD", 18);
    SPAM_ORDER_MAP.put("BADURL_SPAM", 17);
    SPAM_ORDER_MAP.put("BADURL_MALWARE", 16);
    SPAM_ORDER_MAP.put("BADURL_PHISHING", 15);
    SPAM_ORDER_MAP.put("OTHER_SPAM", 14);
    SPAM_ORDER_MAP.put("BULK_SPAM", 13);
    SPAM_ORDER_MAP.put("PROFANITY", 12);
    SPAM_ORDER_MAP.put("PORN", 11);
    SPAM_ORDER_MAP.put("UCV_SPAMMER", 10);
    SPAM_ORDER_MAP.put("USER_FLAGGED_SPAM", 9);
    SPAM_ORDER_MAP.put("COMMENT_META", 8);
    SPAM_ORDER_MAP.put("DUPLICATES", 7);
    SPAM_ORDER_MAP.put("PROMOTION", 6);
    SPAM_ORDER_MAP.put("EVENT", 5);
    SPAM_ORDER_MAP.put("JOB", 4);
    SPAM_ORDER_MAP.put("NEW_MEMBER_LIX_BLOCKED", 3);
    SPAM_ORDER_MAP.put("UBIQUITY_SPAM", 2);
    SPAM_ORDER_MAP.put("UBIQUITY_LOW_QUALITY", 1);
  }

  @SuppressWarnings({ "resource", "unchecked" })
  public static void main(String[] args) throws IOException {

    Configuration hadoopConf = new Configuration();
    FileSystem fs = FileSystem.get(hadoopConf);
    fs.deleteOnExit(new Path("/user/sidas/spark_cste"));

    SparkConf conf = new SparkConf().setAppName("SparkContentFilteringSpam");
    JavaSparkContext sc = new JavaSparkContext(conf);
    String path = "/data/tracking/ContentSpamTrackingEvent/daily/2016/02/06";

    //val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path);

    JavaPairRDD<AvroKey<GenericRecord>, NullWritable> dataRDD =
        sc.newAPIHadoopFile(path, AvroKeyInputFormat.class, AvroKey.class, NullWritable.class, hadoopConf);

    JavaPairRDD<AvroKey<GenericRecord>, NullWritable> bamFilterRDD =
        dataRDD.filter(line -> {
          GenericRecord header = (GenericRecord) line._1.datum().get("header");
          return header.get("service").toString().equals("bam")
              && line._1.datum().get("contentClassificationTrackingId") != null;
        });

    JavaPairRDD<Tuple3<String, String, byte[]>, Tuple5<String, Boolean, String, String, String>> contentInfoRDD =
        bamFilterRDD.mapToPair(bamfilter -> {
          GenericRecord mainRecord = bamfilter._1.datum();
          GenericRecord header = (GenericRecord) mainRecord.get("header");
          GenericRecord metaInfo = (GenericRecord) mainRecord.get("metaInfo");

          String date_sk = epochToDate((Long) header.get("time"), "yyyy-MM-dd", "America/Los_Angeles");
          String contentSource =
              metaInfo.get("contentSource") == null ? "UNKNOWN" : metaInfo.get("contentSource").toString();
          Fixed trackingk = (Fixed) mainRecord.get("contentClassificationTrackingId");
          byte[] trackingId = trackingk.bytes();

            String country =
                metaInfo.get("originCountryCode") == null ? "UNKNOWN" : metaInfo.get("originCountryCode").toString();
            boolean isSpam = (Boolean) mainRecord.get("isSpam");
            String urn = (String) mainRecord.get("contentUrn");
            String systemName = mainRecord.get("system") == null ? "UNKNOWN" : mainRecord.get("system").toString();
            Array<Record> spamOutcome = (Array<Record>) mainRecord.get("spamOutcome");

            String spam = spamOutcome.isEmpty() ? "NA" : ((EnumSymbol) spamOutcome.get(0).get(0)).toString();

            Tuple3<String, String, byte[]> key = new Tuple3<String, String, byte[]>(date_sk, contentSource, trackingId);
            Tuple5<String, Boolean, String, String, String> value =
                new Tuple5<String, Boolean, String, String, String>(country, isSpam, urn, systemName, spam);

            return new Tuple2<Tuple3<String, String, byte[]>, Tuple5<String, Boolean, String, String, String>>(key,
                value);

          });


    JavaPairRDD<Tuple3<String, String, byte[]>, Iterable<Tuple5<String, Boolean, String, String, String>>> groupRDD =
        contentInfoRDD.groupByKey();

    JavaPairRDD<Tuple6<String, String, String, Boolean, String, String>, Integer> spamRDD =
        groupRDD.mapToPair(group -> processSpam(group));

    JavaPairRDD<Tuple6<String, String, String, Boolean, String, String>, Integer> finalRDD =
        spamRDD.reduceByKey((x, y) -> x + y);

    // finalRDD.map(finalRdd -> finalRdd._2).saveAsTextFile("/user/sidas/spark_cste");
    finalRDD.saveAsTextFile("/user/sidas/spark_cste");

    // contentInfoRDD.first();

    sc.close();
  }

  /**
  *
  *Process the record whether this is spam or not
  * @throws IOException
  */

  public static Tuple2<Tuple6<String, String, String, Boolean, String, String>, Integer> processSpam(
      Tuple2<Tuple3<String, String, byte[]>, Iterable<Tuple5<String, Boolean, String, String, String>>> group) {
    Tuple5<String, Boolean, String, String, String> element = null;
    Set<String> systemNameSet = new HashSet<String>(3);
    Set<Boolean> isSpamSet = new HashSet<Boolean>(2);
    Set<String> urnSet = new HashSet<String>(2);
    List<SpamType> spamList = new ArrayList<SpamType>(5);
    String country = "UNKNOWN";
    String contentSource = group._1._2();
    String date_sk = null;
    Iterator<Tuple5<String, Boolean, String, String, String>> it = group._2.iterator();
    while (it.hasNext()) {
      element = it.next();
      isSpamSet.add(element._2());
      systemNameSet.add(element._4());
      urnSet.add(element._3());
      spamList.add(new SpamType(element._5(), (SPAM_ORDER_MAP.get(element._5()) != null ? SPAM_ORDER_MAP.get(element
          ._5()) : 0)));
      if (!"UNKNOWN".equals(element._1())) {
        country = element._1();
      }
      date_sk = element._1();
    }

    // Check the size of all container
    if (systemNameSet.isEmpty()) {
      throw new RuntimeException(" There are no System names in bags ");
    } else if (isSpamSet.isEmpty()) {
      throw new RuntimeException("There are no IsSpam values in bags");
    } else if (urnSet.isEmpty()) {
      throw new RuntimeException("There are no URNs in bags");
    }

    if (isSpamSet.contains(true)) {
      if (spamList.isEmpty()) {
        throw new RuntimeException("SPAM array is Blank");
      }
      Collections.sort(spamList);
    }

    /**
     * If content_source is USCP_COMMENT the convert it to respective
     * domain like activity,article,discussion or decision_board
     */
    if ("USCP_COMMENT".equals(contentSource)) {
      for (String urn : urnSet) {
        /**
         * Take the non random urn from urn set and change to
         * appropriate content source
         */
        if (!urn.contains("urn:li:randomUrn:")) {
          if (urn.contains("activity")) {
            contentSource = "USCP_COMMENT_ACTIVITY";
          } else if (urn.contains("article")) {
            contentSource = "USCP_COMMENT_ARTICLE";
          } else if (urn.contains("discussion")) {
            contentSource = "USCP_COMMENT_DISCUSSION";
          } else if (urn.contains("decisionBoardCard")) {
            contentSource = "USCP_COMMENT_DECISION_BOARD_CARD";
          } else if (urn.contains("decisionBoard")) {
            contentSource = "USCP_COMMENT_DECISION_BOARD";
          } else if (urn.contains("treasurymedia")) {
            contentSource = "USCP_COMMENT_TREASURY_MEDIA";
          } else {
            contentSource = "USCP_COMMENT";
          }
          break;
        }
      }
    }

    /**
     * Prepare return Tuple as content_source,isSpam,system_name,spam_type and country.
     */

    String systemName = null;
    if (systemNameSet.contains("HUMAN")) {
      systemName = "HUMAN";
    } else {
      String[] systemArr = new String[systemNameSet.size()];
      systemNameSet.toArray(systemArr);
      Comparator<String> cmp = Collections.reverseOrder();
      Arrays.sort(systemArr, cmp);
      systemName = systemArr[0];
    }
    Tuple6<String, String, String, Boolean, String, String> key =
        new Tuple6<String, String, String, Boolean, String, String>(group._1._1(), country, contentSource,
            isSpamSet.contains(true) ? true : false, systemName, isSpamSet.contains(true) ? spamList.get(0).getSpam()
                : "NA");
    return new Tuple2<Tuple6<String, String, String, Boolean, String, String>, Integer>(key, 1);

  }

  /**
   *
   *Convert epoch to date
   *
   * @throws IOException
   */

  public static String epochToDate(long time, String timeFormat, String timeZone) throws IOException {
    SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd");
    try {
      dateParser.applyPattern(timeFormat);
      dateParser.setTimeZone(TimeZone.getTimeZone(timeZone));
      return dateParser.format(time);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception processing input row ", e);
    }
  }

  /** Inner class for SPAM Type declaration */
  private static class SpamType implements Comparable<SpamType> {
    private String spam;
    private Integer capacity;

    SpamType(String spam, Integer capacity) {
      this.spam = spam;
      this.capacity = capacity;
    }

    public String getSpam() {
      return spam;
    }

    public Integer getCapacity() {
      return capacity;
    }

    @Override
    public int compareTo(SpamType spamType) {
      int compareCapacity = spamType.getCapacity();
      // descending order
      return compareCapacity - this.capacity;
    }

    @Override
    public String toString() {
      return "{spam:" + spam + ",capacity:" + capacity + "}";
    }
  }
}

package main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SparkDemo {
    public static void main(String[] args) {
        String brokerUrl = "tcp://10.129.149.9:1883";
        String topic = "data/kresit/sch/3";
        SparkConf sparkConf = new SparkConf().setAppName("main.SparkDemo")
                .set("spark.sql.warehouse.dir", "~/Desktop/spark-warehouse")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.allowMultipleContexts", "true");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]");
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final SQLContext sqlContext= new SQLContext(sc);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(3000));
        jssc.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK());
        final Function<String, Row> stringRowFunction = new Function<String, Row>() {
            public Row call(String s) throws Exception {
                String[] strList = s.split(",");
                Float[] floatList = new Float[strList.length];
                for (int i = 0; i < strList.length; i++) {
                    floatList[i] = Float.parseFloat(strList[i]);
                }
                return RowFactory.create(floatList);
            }
        };
        messages.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call_(JavaRDD<String> stringJavaRDD, Time time) {

            }

            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                System.out.println(time.toString());
                JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(stringRowFunction);
                Dataset<Row> rows = sqlContext.applySchema(rowJavaRDD, getDataSchema());
                rows.show();

                if(rows.count()>0)
                    System.out.println(rows.first().getFloat(1));
                HashMap<String,String> aggregationMap = new HashMap<String, String>();
                aggregationMap.put("W","avg");
                Dataset<Row> aggRows = rows.agg(aggregationMap);
                aggRows = aggRows.withColumnRenamed("avg(W)","W");
                aggRows.show();


//                System.out.println(agg.first().toString());
//                System.out.println(stringJavaRDD);
//                agg.printSchema();
//                sqlContext.createDataFrame(rowJavaRDD,getDataSchema());


//                stringJavaRDD.foreach(new VoidFunction<String>() {
//                    public void call(String s) throws Exception {
//                        System.out.println("--------Data-----------");
//                        System.out.println(s);
//
//
//                        Dataset<Row> lines = sqlContext
//                                .readStream()
//                                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
//                                .option("topic", topic)
//                                .schema(getDataSchema())
//                                .option("delimiter", ",")
//                                .load(brokerUrl);
//
//
//
//                    }
//                });
                System.out.println("-----------count = "+stringJavaRDD.count()+" ------------");
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("------------------------------Error--------------------------"+e.getMessage());
        }
    }
    public static StructType getDataSchema(){
        StructField srl = DataTypes.createStructField("srl", DataTypes.FloatType, true);
        StructField timestamp = DataTypes.createStructField("timestamp", DataTypes.FloatType, true);
        StructField VA = DataTypes.createStructField("VA", DataTypes.FloatType, true);
        StructField W = DataTypes.createStructField("W", DataTypes.FloatType, true);
        StructField VAR = DataTypes.createStructField("VAR", DataTypes.FloatType, true);
        StructField PF = DataTypes.createStructField("PF", DataTypes.FloatType, true);
        StructField VLL = DataTypes.createStructField("VLL", DataTypes.FloatType, true);
        StructField VLN = DataTypes.createStructField("VLN", DataTypes.FloatType, true);
        StructField A = DataTypes.createStructField("A", DataTypes.FloatType, true);
        StructField F = DataTypes.createStructField("F", DataTypes.FloatType, true);
        StructField VA1 = DataTypes.createStructField("VA1", DataTypes.FloatType, true);
        StructField W1 = DataTypes.createStructField("W1", DataTypes.FloatType, true);
        StructField VAR1 = DataTypes.createStructField("VAR1", DataTypes.FloatType, true);
        StructField PF1 = DataTypes.createStructField("PF1", DataTypes.FloatType, true);
        StructField V12 = DataTypes.createStructField("V12", DataTypes.FloatType, true);
        StructField V1 = DataTypes.createStructField("V1", DataTypes.FloatType, true);
        StructField A1 = DataTypes.createStructField("A1", DataTypes.FloatType, true);
        StructField VA2 = DataTypes.createStructField("VA2", DataTypes.FloatType, true);
        StructField W2 = DataTypes.createStructField("W2", DataTypes.FloatType, true);
        StructField VAR2 = DataTypes.createStructField("VAR2", DataTypes.FloatType, true);
        StructField PF2 = DataTypes.createStructField("PF2", DataTypes.FloatType, true);
        StructField V23 = DataTypes.createStructField("V23", DataTypes.FloatType, true);
        StructField V2 = DataTypes.createStructField("V2", DataTypes.FloatType, true);
        StructField A2 = DataTypes.createStructField("A2", DataTypes.FloatType, true);
        StructField VA3 = DataTypes.createStructField("VA3", DataTypes.FloatType, true);
        StructField W3 = DataTypes.createStructField("W3", DataTypes.FloatType, true);
        StructField VAR3 = DataTypes.createStructField("VAR3", DataTypes.FloatType, true);
        StructField PF3 = DataTypes.createStructField("PF3", DataTypes.FloatType, true);
        StructField V31 = DataTypes.createStructField("V31", DataTypes.FloatType, true);
        StructField V3 = DataTypes.createStructField("V3", DataTypes.FloatType, true);
        StructField A3 = DataTypes.createStructField("A3", DataTypes.FloatType, true);
        StructField FwdVAh = DataTypes.createStructField("FwdVAh", DataTypes.FloatType, true);
        StructField FwdWh = DataTypes.createStructField("FwdWh", DataTypes.FloatType, true);
        StructField FwdVARhR = DataTypes.createStructField("FwdVARhR", DataTypes.FloatType, true);
        StructField FwdVARhC = DataTypes.createStructField("FwdVARhC", DataTypes.FloatType, true);


        List<StructField> fields = Arrays.asList(srl,timestamp,VA,W,VAR,PF,
                VLL, VLN, A ,F,VA1,W1,
                VAR1, PF1, V12, V1, A1,
                VA2, W2, VAR2, PF2,V23,V2,
                A2,VA3,W3,VAR3,PF3,V31,V3,A3,FwdVAh,FwdWh,FwdVARhR,FwdVARhC);

        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }


}

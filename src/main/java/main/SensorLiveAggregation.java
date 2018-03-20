package main;

import com.sun.xml.bind.v2.TODO;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static main.SparkDemo.getDataSchema;

public class SensorLiveAggregation {

    String sensorId;
    String toTableName;
    double startTS;
    MySQLHandler mySQLHandler;
    String timeField;
    Spark spark;
    Dataset<Row> globalBuffer;
    final Function<String, Row> stringRowFunction = new Function<String, Row>() {
        public Row call(String s) {
            String[] strList = s.split(",");
            Double[] doubleList = new Double[strList.length];
            for (int i = 0; i < strList.length; i++) {
                doubleList[i] = Double.parseDouble(strList[i]);
            }
            return RowFactory.create(doubleList);
        }
    };

    public SensorLiveAggregation(String sensorId, String toTableName) {
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.startTS = -1;
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
        this.spark = new Spark();
//		this.startTS = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp
    }

    public void startAggregation() {
        long startEpoch = System.currentTimeMillis();


        Dataset<Row> rows = fetchDataForAggregation();
        long fetchEndEpoch = System.currentTimeMillis();
        long aggEndEpoch = 0, storeEndEpoch = 0;
        if (rows.count() > 0) {
            rows = aggregateDataUsingSQL(rows);
            aggEndEpoch = System.currentTimeMillis();
            storeAggregatedData(rows);
            storeEndEpoch = System.currentTimeMillis();
        }
        LogHandler.logInfo("[" + sensorId + "]Aggregation ended for minute " + UtilsHandler.tsToStr(this.startTS) + "\n[FetchingTime(" + (fetchEndEpoch - startEpoch) + ")]" +
                "[AggregationTime(" + (aggEndEpoch - startEpoch) + ")]" +
                "[StoringTime(" + (storeEndEpoch - startEpoch) + ")]");
        this.goToNextMinute();
    }

    private String getTopic(String sensorId) {
        return "data/kresit/sch/3"; // TODO return proper topic
    }


    public void goToNextMinute() {
        this.startTS += ConfigHandler.GRANULARITY_IN_SECONDS;
    }

    private Dataset<Row> aggregateDataUsingSQL(Dataset<Row> rows) {
        String[] aggregationFormula = getSQLAggregationFormula(fromTableName);
        rows.createOrReplaceTempView("sensor_data_" + this.sensorId);
        String sql = "select ";
        for (int i = 0; i < aggregationFormula.length; i++) {
            sql = sql + " " + aggregationFormula[i] + ", ";
        }
        sql = sql + startTS + " as " + timeField + "  from sensor_data_" + this.sensorId;
        rows = spark.sparkSession.sql(sql);
        return rows;
    }

    private String[] getSQLAggregationFormula(String tableName) {
        if (tableName.equalsIgnoreCase("sch_3")) {
            return ConfigHandler.SQL_AGGREGATION_FORMULA_SCH;
        } else {
            LogHandler.logError("[AggregationFormula] not found for table: " + tableName);
            return null;
        }
    }

    private void storeAggregatedData(Dataset<Row> rows) {
        rows.write().mode(SaveMode.Append).jdbc(ConfigHandler.MYSQL_URL, toTableName, spark.getProperties());
    }

    private Dataset<Row> fetchDataForAggregation() {
        final JavaSparkContext javaSparkContext = spark.getJavaSparkContext();
        final SQLContext sqlContext  = spark.getSQLContext();
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext,
                new Duration(ConfigHandler.LIVE_GRANULARITY_IN_SECONDS * 1000));
        jssc.checkpoint("live aggregation start checkpoint");
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, ConfigHandler.MQTT_URL, getTopic(sensorId), StorageLevel.MEMORY_AND_DISK());
        messages.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> stringJavaRDD, Time time) {
                JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(stringRowFunction);
                Dataset<Row> rows = sqlContext.applySchema(rowJavaRDD, getDataSchema());
                globalBuffer=globalBuffer.union(rows);

            }
        });
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("------------------------------Error--------------------------" + e.getMessage());
        }
        return null;
    }

    public double getStartTS() {
        return startTS;
    }


}


    public static void main(int a, String[] args) {
        Spark spark = new Spark();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkConf);
        final SQLContext sqlContext = new SQLContext(sc);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(3000));
        jssc.checkpoint("checkpoint");


        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK());
        final Function<String, Row> stringRowFunction = new Function<String, Row>() {
            public Row call(String s) throws Exception {
                String[] strList = s.split(",");
                Double[] doubleList = new Double[strList.length];
                for (int i = 0; i < strList.length; i++) {
                    doubleList[i] = Double.parseDouble(strList[i]);
                }
                return RowFactory.create(doubleList);
            }
        };
        messages.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call_(JavaRDD<String> stringJavaRDD, Time time) {

            }

            public void call(JavaRDD<String> stringJavaRDD, Time time) {
                System.out.println(time.toString());
                JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(stringRowFunction);
                Dataset<Row> rows = sqlContext.applySchema(rowJavaRDD, getDataSchema());
                rows.show();

                if (rows.count() > 0)
                    System.out.println(rows.first().getDouble(1));
                HashMap<String, String> aggregationMap = new HashMap<String, String>();
                aggregationMap.put("W", "avg");
                Dataset<Row> aggRows = rows.agg(aggregationMap);
                aggRows = aggRows.withColumnRenamed("avg(W)", "W");
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
                System.out.println("-----------count = " + stringJavaRDD.count() + " ------------");
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("------------------------------Error--------------------------" + e.getMessage());
        }
    }

    public static StructType getDataSchema() {
        StructField srl = DataTypes.createStructField("srl", DataTypes.DoubleType, true);
        StructField timestamp = DataTypes.createStructField("timestamp", DataTypes.DoubleType, true);
        StructField VA = DataTypes.createStructField("VA", DataTypes.DoubleType, true);
        StructField W = DataTypes.createStructField("W", DataTypes.DoubleType, true);
        StructField VAR = DataTypes.createStructField("VAR", DataTypes.DoubleType, true);
        StructField PF = DataTypes.createStructField("PF", DataTypes.DoubleType, true);
        StructField VLL = DataTypes.createStructField("VLL", DataTypes.DoubleType, true);
        StructField VLN = DataTypes.createStructField("VLN", DataTypes.DoubleType, true);
        StructField A = DataTypes.createStructField("A", DataTypes.DoubleType, true);
        StructField F = DataTypes.createStructField("F", DataTypes.DoubleType, true);
        StructField VA1 = DataTypes.createStructField("VA1", DataTypes.DoubleType, true);
        StructField W1 = DataTypes.createStructField("W1", DataTypes.DoubleType, true);
        StructField VAR1 = DataTypes.createStructField("VAR1", DataTypes.DoubleType, true);
        StructField PF1 = DataTypes.createStructField("PF1", DataTypes.DoubleType, true);
        StructField V12 = DataTypes.createStructField("V12", DataTypes.DoubleType, true);
        StructField V1 = DataTypes.createStructField("V1", DataTypes.DoubleType, true);
        StructField A1 = DataTypes.createStructField("A1", DataTypes.DoubleType, true);
        StructField VA2 = DataTypes.createStructField("VA2", DataTypes.DoubleType, true);
        StructField W2 = DataTypes.createStructField("W2", DataTypes.DoubleType, true);
        StructField VAR2 = DataTypes.createStructField("VAR2", DataTypes.DoubleType, true);
        StructField PF2 = DataTypes.createStructField("PF2", DataTypes.DoubleType, true);
        StructField V23 = DataTypes.createStructField("V23", DataTypes.DoubleType, true);
        StructField V2 = DataTypes.createStructField("V2", DataTypes.DoubleType, true);
        StructField A2 = DataTypes.createStructField("A2", DataTypes.DoubleType, true);
        StructField VA3 = DataTypes.createStructField("VA3", DataTypes.DoubleType, true);
        StructField W3 = DataTypes.createStructField("W3", DataTypes.DoubleType, true);
        StructField VAR3 = DataTypes.createStructField("VAR3", DataTypes.DoubleType, true);
        StructField PF3 = DataTypes.createStructField("PF3", DataTypes.DoubleType, true);
        StructField V31 = DataTypes.createStructField("V31", DataTypes.DoubleType, true);
        StructField V3 = DataTypes.createStructField("V3", DataTypes.DoubleType, true);
        StructField A3 = DataTypes.createStructField("A3", DataTypes.DoubleType, true);
        StructField FwdVAh = DataTypes.createStructField("FwdVAh", DataTypes.DoubleType, true);
        StructField FwdWh = DataTypes.createStructField("FwdWh", DataTypes.DoubleType, true);
        StructField FwdVARhR = DataTypes.createStructField("FwdVARhR", DataTypes.DoubleType, true);
        StructField FwdVARhC = DataTypes.createStructField("FwdVARhC", DataTypes.DoubleType, true);


        List<StructField> fields = Arrays.asList(srl, timestamp, VA, W, VAR, PF,
                VLL, VLN, A, F, VA1, W1,
                VAR1, PF1, V12, V1, A1,
                VA2, W2, VAR2, PF2, V23, V2,
                A2, VA3, W3, VAR3, PF3, V31, V3, A3, FwdVAh, FwdWh, FwdVARhR, FwdVARhC);

        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

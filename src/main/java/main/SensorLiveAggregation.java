package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import sun.security.ssl.Debug;

import javax.rmi.CORBA.Util;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class SensorLiveAggregation implements Serializable {

    String sensorId;
    String toTableName;
    double startTs;
    double endTs;
    MySQLHandler mySQLHandler;
    String timeField;
    Spark spark;
    Dataset<Row> globalBuffer;
    //    Dataset<Row> globalAggregatedBuffer;
    String tableNameForSchema;

    public SensorLiveAggregation(String tableNameForSchema, String sensorId, String toTableName) {
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.tableNameForSchema = tableNameForSchema;
        this.startTs = -1;
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
        this.spark = new Spark();
//		this.startTs = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp
    }


    public void goToNextInterval() {
        this.startTs += ConfigHandler.LIVE_GRANULARITY_IN_SECONDS;
    }

    private Dataset<Row> aggregateDataUsingSQL(Dataset<Row> rows) {
        String[] aggregationFormula = getSQLAggregationFormula(tableNameForSchema);
        rows.createOrReplaceTempView("sensor_data_" + this.sensorId);
        String sql = "select ";
        for (int i = 0; i < aggregationFormula.length; i++) {
            sql = sql + " " + aggregationFormula[i] + ", ";
        }
        sql = sql + startTs + " as " + timeField + "  from sensor_data_" + this.sensorId;
        return spark.sparkSession.sql(sql);
    }

    private String[] getSQLAggregationFormula(String tableName) {
        if (tableName.equalsIgnoreCase("sch_3")) {
            return ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3;
        } else {
            LogHandler.logError("[AggregationFormula] not found for table: " + tableName);
            return null;
        }
    }

    private StructType getLiveDataSchema(String tableName) {
        if (tableName.equalsIgnoreCase("sch_3")) {
            return ConfigHandler.SCH_3_SCHEMA;
        } else {
            LogHandler.logError("[LiveDataSchema] not found for table: " + tableName);
            return null;
        }
    }

    private void storeAggregatedData(Dataset<Row> rows) {
        rows.write().mode(SaveMode.Append).jdbc(ConfigHandler.MYSQL_URL, toTableName, spark.getProperties());
    }

    public Dataset<Row> startAggregation() {
        String topic = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[Topic(" + topic + ")]");


        // Subscribe to 1 topic
        Dataset<Row> stream = spark.sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.129.149.18:9092,10.129.149.19:9092,10.129.149.20:9092")
//                .option("startingOffsets", "earliest")
                .option("subscribe", "data")
                .load();
        Dataset<Row> key_value = stream.select(functions.col("key").cast("string"),
                functions.col("value").cast("string"));
        Dataset<Row> topic_key_value = key_value.select("key","value").where(functions.col("key").equalTo(topic));
        Dataset<Row> value = topic_key_value.select("value");
//TODO        value.flatMap(new , Encoders.DOUBLE());

        StreamingQuery console = value.writeStream()
                .format("console")
                .start();
        try {
            console.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Dataset<Row> storeAggregatedDataInBuffer(Dataset<Row> rowsDataset, Dataset<Row> aggregatedBuffer) {
        if (aggregatedBuffer == null) {
            return rowsDataset;
        } else {
            return aggregatedBuffer.union(rowsDataset);
        }
    }

    public Dataset<Row> fetchDataForAggregation(Dataset<Row> aggregableBuffer) {
        Dataset<Row> rows = aggregableBuffer.where(timeField + " >= " + startTs + " and " + timeField + " < " + (startTs + ConfigHandler.LIVE_GRANULARITY_IN_SECONDS));
        return rows;
    }


    public double getStartTs() {
        return startTs;
    }
}

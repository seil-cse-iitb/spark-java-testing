package main;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import javax.rmi.CORBA.Util;
import java.io.Serializable;
import java.util.ArrayList;

public class SensorLiveAggregation implements Serializable {

    String sensorId;
    String toTableName;
    double startTs;
    double endTs;
    MySQLHandler mySQLHandler;
    String timeField;
    Spark spark;
    Dataset<Row> globalBuffer;
    Dataset<Row> globalAggregatedBuffer;
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
        final JavaSparkContext javaSparkContext = spark.getJavaSparkContext();
        final SQLContext sqlContext = spark.getSQLContext();
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext,
                new Duration(ConfigHandler.LIVE_AGGREGATION_INTERVAL_IN_SECONDS * 1000));
        jssc.checkpoint("checkpoint");
        String topic = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[Topic(" + topic + ")]");
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, ConfigHandler.MQTT_URL, topic, StorageLevel.MEMORY_AND_DISK());
        final Function<String, Row> stringRowFunction = new Function<String, Row>() {
            public Row call(String s) {
                String[] strArray = s.split(",");
                ArrayList<Object> list = new ArrayList<Object>();
                double ts_recv = System.currentTimeMillis() / 1000;
                list.add(sensorId);
                list.add(ts_recv);
                for (int i = 0; i < strArray.length; i++) {
                    list.add(Double.parseDouble(strArray[i]));
                }
                return RowFactory.create(list.toArray());
            }
        };

        messages.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> stringJavaRDD, Time time) {
                JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(stringRowFunction);
                if (rowJavaRDD.isEmpty()) {
                    LogHandler.logInfo("[rowJavaRDD Empty]");
                    return;
                }
                Dataset<Row> rows = sqlContext.applySchema(rowJavaRDD, getLiveDataSchema(tableNameForSchema));
                Dataset<Row> select = rows.select(functions.min(timeField), functions.max(timeField));
                Row maxMinTs = select.first();
                double minTs = maxMinTs.getDouble(maxMinTs.fieldIndex("min(" + timeField + ")"));
                double maxTs = maxMinTs.getDouble(maxMinTs.fieldIndex("max(" + timeField + ")"));
                if (globalBuffer == null) {
                    if (minTs % ConfigHandler.LIVE_GRANULARITY_IN_SECONDS != 0) {
                        minTs = (minTs - (minTs % ConfigHandler.LIVE_GRANULARITY_IN_SECONDS)) + ConfigHandler.LIVE_GRANULARITY_IN_SECONDS;
                    }
                    globalBuffer = rows.where(timeField + ">=" + minTs);//Ignore part
                    startTs = minTs;
                } else {
                    globalBuffer = globalBuffer.union(rows);
                }
                endTs = maxTs - maxTs % ConfigHandler.LIVE_GRANULARITY_IN_SECONDS;
                long aggregableBatchAvailable = ((long) ((endTs - startTs) / ConfigHandler.LIVE_GRANULARITY_IN_SECONDS));
                Dataset<Row> aggregableBuffer = globalBuffer.where(timeField + "<" + endTs);
                globalBuffer = globalBuffer.where(timeField + ">=" + endTs);
                globalBuffer.persist();
                LogHandler.logInfo("[" + sensorId + "][startTs(" + UtilsHandler.tsToStr(startTs) + ")]" + "[endTs(" + UtilsHandler.tsToStr(endTs) + ")]"
                        + "[aggregableBatchAvailable(" + aggregableBatchAvailable + ")]");
                if (aggregableBatchAvailable <= 0) {
                    return;
                }
                //now aggregate the aggregableBuffer and store into aggregatedBuffer
                Dataset<Row> aggregatedBuffer = null;
                for (int i = 0; startTs != endTs; goToNextInterval()) {
                    long startEpoch = System.currentTimeMillis();
                    Dataset<Row> rowsDataset = fetchDataForAggregation(aggregableBuffer);
                    long fetchEndEpoch = System.currentTimeMillis();
                    long aggEndEpoch = 0, storeInBufferEndEpoch = 0;
                    Dataset<Row> aggregatedDataset = aggregateDataUsingSQL(rowsDataset);
                    aggEndEpoch = System.currentTimeMillis();
                    if (aggregatedBuffer == null) {
                        aggregatedBuffer = aggregatedDataset;
                    } else {
                        aggregatedBuffer = aggregatedBuffer.union(aggregatedDataset);
                    }
                    storeInBufferEndEpoch = System.currentTimeMillis();
                    LogHandler.logInfo("[" + sensorId + "][AggregationDoneFor(" + UtilsHandler.tsToStr(startTs) + ")]"
                            + "[FetchingTime(" + (fetchEndEpoch - startEpoch) + ")]" + "[AggregationTime(" + (aggEndEpoch - startEpoch) + ")]"
                            + "[StoringInBufferTime(" + (storeInBufferEndEpoch - startEpoch) + ")]");
                }
                long storeEndEpoch = 0;
                long startEpoch = System.currentTimeMillis();
                aggregatedBuffer.show();
                storeAggregatedData(aggregatedBuffer);
                storeEndEpoch = System.currentTimeMillis();
                LogHandler.logInfo("[StoringTime(" + (storeEndEpoch - startEpoch) + ")]");
                // BlockRDD[1] claimed: Error solved by persisting the dataset
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

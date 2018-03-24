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
import scala.concurrent.Future;

import java.io.Serializable;
import java.util.ArrayList;

public class SensorLiveAggregation implements Serializable {

    String sensorId;
    String toTableName;
    double startTS;
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
        this.startTS = -1;
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
        this.spark = new Spark();
//		this.startTS = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp
    }


    public void goToNextMinute() {
        this.startTS += ConfigHandler.LIVE_GRANULARITY_IN_SECONDS;
    }

    private Dataset<Row> aggregateDataUsingSQL(Dataset<Row> rows) {
        String[] aggregationFormula = getSQLAggregationFormula(tableNameForSchema);
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
        String topic  = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[Topic]"+topic);
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, ConfigHandler.MQTT_URL, topic, StorageLevel.MEMORY_AND_DISK());
        final Function<String, Row> stringRowFunction = new Function<String, Row>() {
            public Row call(String s) throws Exception {
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
                if (globalBuffer == null) {
                    globalBuffer = rows;
                } else {
                    globalBuffer = globalBuffer.union(rows);
                }
                Row maxMinTs = globalBuffer.select(functions.max(timeField), functions.min(timeField)).first();
                double maxTs =  maxMinTs.getDouble(maxMinTs.fieldIndex("max(" + timeField + ")"));
                double minTs =  maxMinTs.getDouble(maxMinTs.fieldIndex("min(" + timeField + ")"));
                long aggregableBatchAvailable = ((long) (maxTs / ConfigHandler.LIVE_GRANULARITY_IN_SECONDS)) - ((long) (minTs / ConfigHandler.LIVE_GRANULARITY_IN_SECONDS));
                LogHandler.logInfo("[minTS("+minTs+")][maxTS("+maxTs+")][aggregableBatchAvailable("+aggregableBatchAvailable+")]");
                if (aggregableBatchAvailable == 0){
                    globalBuffer.persist();
                    return;//don't aggregate or ignore if no complete batch is available
                    }
                if (minTs % ConfigHandler.LIVE_GRANULARITY_IN_SECONDS != 0) {
                    //Ignore part
                    minTs = (minTs - minTs % ConfigHandler.LIVE_GRANULARITY_IN_SECONDS) + ConfigHandler.LIVE_GRANULARITY_IN_SECONDS;
                    globalBuffer = globalBuffer.where(timeField + ">=" + minTs);
                }
                Dataset<Row> except = globalBuffer.where(timeField + ">=" + ((aggregableBatchAvailable * ConfigHandler.LIVE_GRANULARITY_IN_SECONDS) + minTs));
                Dataset<Row> aggregableBuffer = globalBuffer.except(except);
                globalBuffer = except;
                globalBuffer.persist();
                //now aggregate the aggregableBuffer
                startTS = minTs;
                for (int i = 0; i < aggregableBatchAvailable; i++) {
                    long startEpoch = System.currentTimeMillis();
                    Dataset<Row> rowsDataset = fetchDataForAggregation(aggregableBuffer);
                    long fetchEndEpoch = System.currentTimeMillis();
                    long aggEndEpoch = 0, storeInBufferEndEpoch = 0;
                    rowsDataset = aggregateDataUsingSQL(rowsDataset);
                    aggEndEpoch = System.currentTimeMillis();
                    storeAggregatedDataInBuffer(rowsDataset);
                    storeInBufferEndEpoch = System.currentTimeMillis();
                    LogHandler.logInfo("[" + sensorId + "]Aggregation ended for interval " + UtilsHandler.tsToStr(startTS) + "\n[FetchingTime(" + (fetchEndEpoch - startEpoch) + ")]" +
                            "[AggregationTime(" + (aggEndEpoch - startEpoch) + ")]" + "[StoringInBufferTime(" + (storeInBufferEndEpoch - startEpoch) + ")]\n" +
                            "Aggregable batch remaining[" + (aggregableBatchAvailable - i - 1) + "]");
                    goToNextMinute();
                }
                long storeEndEpoch = 0;
                long startEpoch = System.currentTimeMillis();
                storeAggregatedData(globalAggregatedBuffer);
                globalAggregatedBuffer = null;
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

    private void storeAggregatedDataInBuffer(Dataset<Row> rowsDataset) {
        if (this.globalAggregatedBuffer == null) {
            this.globalAggregatedBuffer = rowsDataset;
        } else {
            this.globalAggregatedBuffer = this.globalAggregatedBuffer.union(rowsDataset);
        }
    }

    public Dataset<Row> fetchDataForAggregation(Dataset<Row> aggregableBuffer) {
        Dataset<Row> rows = aggregableBuffer.where(timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.LIVE_GRANULARITY_IN_SECONDS));
        return rows;
    }


    public double getStartTS() {
        return startTS;
    }
}

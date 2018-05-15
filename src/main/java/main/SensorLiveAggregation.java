package main;

import handlers.ConfigHandler;
import handlers.LogHandler;
import handlers.UtilsHandler;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;

public class SensorLiveAggregation implements Serializable {

    String sensorId;
    String toTableName;
    double startTs;
    String timeField;
    Spark spark;
    String tableNameForSchema;

    public SensorLiveAggregation(String tableNameForSchema, String sensorId, String toTableName) {
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.tableNameForSchema = tableNameForSchema;
        this.startTs = -1;
        this.spark = new Spark();
//		this.startTs = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp
    }

    public Dataset<Row> startAggregationKafka() {
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
        Dataset<Row> topic_key_value = key_value.select("key", "value").where(functions.col("key").equalTo(topic));
        Dataset<Row> value = topic_key_value.select("value");
//        Dataset<Row> rowDataset = value.withColumn("temp", functions.split(functions.col("value"), ","));
        printOnConsole(topic_key_value);


        //TODO        value.flatMap(new , Encoders.DOUBLE());
        return null;
    }

    public void startAggregation() {
        String topic = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[Topic(" + topic + ")]");
        Dataset<Row> stream = spark.sparkSession
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", topic)
                .load(ConfigHandler.MQTT_URL);
        stream.printSchema();
        Dataset<Row> sch3 = spark.sparkSession.createDataFrame(new ArrayList<Row>(), ConfigHandler.SCH_3_SCHEMA);
        ExpressionEncoder<Row> rowExpressionEncoder = sch3.exprEnc();
        Dataset<Row> dataset = stream.map(new MapFunction<Row, Row>() {
            public Row call(Row row) throws Exception {
                Timestamp timestamp = row.getTimestamp(row.fieldIndex("timestamp"));
                String value1 = row.getString(row.fieldIndex("value"));
                String[] split = value1.split(",");
                ArrayList<Object> rowValues = new ArrayList<Object>();
                rowValues.add(sensorId);
                rowValues.add((double) timestamp.getTime() / 1000);
                for (String s : split) {
                    rowValues.add(Double.parseDouble(s));
                }
                return RowFactory.create(rowValues.toArray());
            }
        }, rowExpressionEncoder);
        dataset.printSchema();
        Dataset<Row> aggregatedData = aggregate(dataset);

        printOnConsole(aggregatedData.select("window", "sensor_id", "W"));
    }

    private Dataset<Row> aggregate(Dataset<Row> dataset) {
        String aggStr = "";
        Column[] aggArray = new Column[ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3.length - 1];
        for (int i = 0; i < ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3.length - 1; i++)
            aggArray[i] = functions.expr(ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3[i + 1]);
        Column expr = functions.expr(ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3[0]);
        Column timestamp = functions.col(timeField).cast(DataTypes.TimestampType).as("eventTime");
        String[] columnStrs = dataset.columns();
        Column[] columns = new Column[columnStrs.length + 1];
        for (int i=0;i<columnStrs.length;i++) {
            columns[i]=functions.col(columnStrs[i]);
        }
        columns[columns.length-1]=timestamp;
        Dataset<Row> select = dataset.select(columns);
        return select.withWatermark("eventTime", "10 minutes").groupBy(functions.window(timestamp, "1 minute"))
                .agg(expr, aggArray);
    }

    private void printOnConsole(Dataset<Row> dataset) {
        StreamingQuery console = dataset.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .option("truncate", false)
                .start();
        try {
            console.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

}

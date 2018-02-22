package main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class Sensor {

    String fromTableName;
    String sensorId;
    String toTableName;
    double startTS;
    MySQLHandler mySQLHandler;
    String timeField;
    Spark spark;

    public Sensor(String fromTableName, String sensorId, String toTableName) {
        this.fromTableName = fromTableName;
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.startTS = -1;
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
        this.spark = new Spark();
        fetchStartTimestamp();

    }

    public void fetchStartTimestamp() {
        try {
            String sql = "select " + timeField + " from " + toTableName + " order by " + this.timeField + " desc limit 1";
            ResultSet resultSet = mySQLHandler.query(sql);
            if (resultSet.next()) {
                this.startTS = resultSet.getDouble(timeField) + 1; //+1 because we want to aggregate from the next second
            } else {
                resultSet.close();
                //toTableName is empty table then fetch first ts from fromTableName
                sql = "select " + timeField + " from " + fromTableName + " order by " + this.timeField + " asc limit 1";
                resultSet = mySQLHandler.query(sql);
                if (resultSet.next()) {
                    this.startTS = resultSet.getDouble(timeField); //No +1 because we want to aggregate from this second itself
                } else {
                    this.startTS = UtilsHandler.tsInSeconds(2016, 10, 1, 0, 0, 0);//base timestamp
                }
            }
            this.startTS = this.startTS - this.startTS % ConfigHandler.AGGREGATION_RANGE_IN_SECONDS;
        } catch (SQLException e) {
            LogHandler.logError("[MySQL][Query][SensorClass]" + e.getMessage());
            UtilsHandler.exit_thread();
        }
    }


    public void startArchivalAggregation() {
        /*
        This function starts aggregation of archival data after the last aggregated row present corresponding to this sensor
         */
        Dataset<Row> rows = fetchDataForAggregation();
        rows.show();
        rows = aggregateData(rows);
        rows.show();
        storeAggregatedData(rows);
    }

    private Dataset<Row> aggregateData(Dataset<Row> rows) {
        HashMap<String,String> aggregationMap = getAggregationMap(fromTableName);
        rows = rows.agg(aggregationMap);
        rows.withColumnRenamed("avg(W)","W");
        return rows;
    }

    private HashMap<String, String> getAggregationMap(String tableName) {
        HashMap<String,String> aggregationMap = new HashMap<String, String>();
        aggregationMap.put("W","avg");
        aggregationMap.put("W1","avg");
        aggregationMap.put("W2","avg");
        aggregationMap.put("W3","avg");
        aggregationMap.put("V1","avg");
        aggregationMap.put("V2","avg");
        aggregationMap.put("V3","avg");
        aggregationMap.put("A","avg");
        aggregationMap.put("A1","avg");
        aggregationMap.put("A2","avg");
        aggregationMap.put("A3","avg");
        return aggregationMap;
    }

    private void storeAggregatedData(Dataset<Row> rows) {

    }
    private Dataset<Row> fetchDataForAggregation() {
        Dataset<Row> rows = getRows(fromTableName);
        rows = rows.where("sensor_id="+"'"+sensorId+"' and "+
                timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.AGGREGATION_RANGE_IN_SECONDS));
        return rows;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public double getStartTS() {
        return startTS;
    }

    public void setStartTS(double startTS) {
        this.startTS = startTS;
    }


    private Dataset<Row> getRows(String tableName){
        Properties properties = new Properties();
        properties.setProperty("user", mySQLHandler.username);
        properties.setProperty("password", mySQLHandler.password);
        Dataset<Row> rows = spark.sparkSession.read().jdbc(mySQLHandler.url, tableName, properties);
        return  rows;
    }
}

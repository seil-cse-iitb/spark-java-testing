package main;

import org.apache.kerby.config.Conf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
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
        fetch1mData();
        store1mAggregatedData();
    }

    private void store1mAggregatedData() {

    }

    private void fetch1mData() {
        this.startTS=UtilsHandler.tsInSeconds(2017, 10, 1, 0, 0, 0);//base timestamp
        Properties properties = new Properties();
        properties.setProperty("user", mySQLHandler.username);
        properties.setProperty("password", mySQLHandler.password);
        Dataset<Row> rows = spark.sqlContext.read().jdbc(mySQLHandler.url, fromTableName, properties);
        rows = rows.where("sensor_id="+"'"+sensorId+"' and "+
                timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.AGGREGATION_RANGE_IN_SECONDS));
        
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
}

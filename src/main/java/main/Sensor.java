package main;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
//		this.startTS = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp

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
        this.fetchStartTimestamp();
		Dataset<Row> rows = fetchDataForAggregation();
		rows = aggregateData(rows);
		storeAggregatedData(rows);
	}

	private Dataset<Row> aggregateData(Dataset<Row> rows) {
		HashMap<String, String> aggregationMap = getAggregationMap(fromTableName);
		rows.createOrReplaceTempView("sensor_data");
		String sql = "select";
		Set<String> keySet = aggregationMap.keySet();
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext(); ) {
			String key = iterator.next();
			String value = aggregationMap.get(key);
			String valueAsKey = " " + value + " as " + key + ",";
			sql = sql + valueAsKey;
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + " from sensor_data";
//		LogHandler.logInfo("[AggregationQuery]" + sql);
		rows = spark.sparkSession.sql(sql);
		return rows;
	}

	private HashMap<String, String> getAggregationMap(String tableName) {
		HashMap<String, String> aggregationMap = new HashMap<String, String>();
		aggregationMap.put("sensor_id", "first(sensor_id)");
		aggregationMap.put("TS_RECV", "last(TS_RECV)");
		aggregationMap.put("TS", (startTS + ConfigHandler.AGGREGATION_RANGE_IN_SECONDS)+"");
		aggregationMap.put("W", "avg(W)");
		aggregationMap.put("W1", "avg(W1)");
		aggregationMap.put("W2", "avg(W2)");
		aggregationMap.put("W3", "avg(W3)");
		aggregationMap.put("V1", "avg(V1)");
		aggregationMap.put("V2", "avg(V2)");
		aggregationMap.put("V3", "avg(V3)");
		aggregationMap.put("A", "avg(A)");
		aggregationMap.put("A1", "avg(A1)");
		aggregationMap.put("A2", "avg(A2)");
		aggregationMap.put("A3", "avg(A3)");
		aggregationMap.put("VA", "avg(VA)");
		aggregationMap.put("VA1", "avg(VA1)");
		aggregationMap.put("VA2", "avg(VA2)");
		aggregationMap.put("VA3", "avg(VA3)");
		aggregationMap.put("PF", "avg(W)/avg(VA)");
		aggregationMap.put("PF1", "avg(W1)/avg(VA1)");
		aggregationMap.put("PF2", "avg(W2)/avg(VA2)");
		aggregationMap.put("PF3", "avg(W3)/avg(VA3)");
		aggregationMap.put("FwdWh", "last(FwdWh)");
		aggregationMap.put("delta_FwdWh", "last(FwdWh)-first(FwdWh)");
		aggregationMap.put("data_percent"
				, "count(*)/"+(ConfigHandler.AGGREGATION_RANGE_IN_SECONDS/ConfigHandler.RATE_OF_DATA_PER_SECOND)+"*100");
		return aggregationMap;
	}

	private void storeAggregatedData(Dataset<Row> rows) {
		DataFrameWriter<Row> df =  new DataFrameWriter<Row>(rows);
		df.mode("append").jdbc(mySQLHandler.url,toTableName,spark.getProperties());
	}

	private Dataset<Row> fetchDataForAggregation() {
		Dataset<Row> rows = spark.getRowsByTableName(fromTableName);
		rows = rows.where("sensor_id=" + "'" + sensorId + "' and " +
				timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.AGGREGATION_RANGE_IN_SECONDS));
		rows = rows.sort(timeField);
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

}

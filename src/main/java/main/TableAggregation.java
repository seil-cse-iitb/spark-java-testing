package main;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableAggregation {

	String fromTableName;
	String toTableName;
	double startTS;
	MySQLHandler mySQLHandler;
	String timeField;
	Spark spark;
	Dataset<Row> fromTableRows;

	public TableAggregation(String fromTableName, String toTableName) {
		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		this.timeField = "ts";
		this.startTS = -1;
		mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
		this.spark = new Spark();
		fetchStartTimestamp();
		fromTableRows = spark.getRowsByTableName(fromTableName);
	}

	public void fetchStartTimestamp() {
		double baseTimestamp = UtilsHandler.tsInSeconds(2016, 10, 1, 0, 0, 0);//base timestamp
		try {
			String sql = "select max(" + timeField + ") from " + toTableName;
			ResultSet resultSet = mySQLHandler.query(sql);
			resultSet.next();
			this.startTS = resultSet.getDouble("max(" + timeField + ")");
			if (this.startTS > baseTimestamp) {
				this.startTS += ConfigHandler.GRANULARITY_IN_SECONDS;
			} else {
				resultSet.close();
				//toTableName is empty table then fetch first ts from fromTableName
				sql = "select min(" + timeField + ") from " + fromTableName;
				resultSet = mySQLHandler.query(sql);
				resultSet.next();
				this.startTS = resultSet.getDouble("min(" + timeField + ")");
				if (this.startTS < baseTimestamp) {
					this.startTS = baseTimestamp;
				}
			}
			this.startTS = this.startTS - this.startTS % ConfigHandler.GRANULARITY_IN_SECONDS;
		} catch (SQLException e) {
			LogHandler.logError("[MySQL][Query][TableAggregation][FetchStartTimestamp]" + e.getMessage());
			UtilsHandler.exit_thread();
		}
	}


	public void startArchivalAggregation() {
        /*
        This function starts aggregation of archival data after the last aggregated row present corresponding to this sensor
         */
//		LogHandler.logInfo("[" + fromTableName + "]Aggregation started for minute " + UtilsHandler.tsToStr(this.startTS));
		if (this.startTS > UtilsHandler.tsInSeconds(2018, 2, 26, 0, 0, 0)) {
			return;
		}
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
		LogHandler.logInfo("["+fromTableName+"]Aggregation ended for minute "+ UtilsHandler.tsToStr(this.startTS)+"\n[FetchingTime(" + (fetchEndEpoch - startEpoch) + ")]" +
				"[AggregationTime(" + (aggEndEpoch - startEpoch) + ")]" +
				"[StoringTime(" + (storeEndEpoch - startEpoch) + ")]");		this.goToNextMinute();
	}

	public void goToNextMinute() {
		this.startTS += ConfigHandler.GRANULARITY_IN_SECONDS;
	}

	private Dataset<Row> aggregateDataUsingSQL(Dataset<Row> rows) {
		String[] aggregationFormula = getSQLAggregationFormula(fromTableName);
		rows.createOrReplaceTempView("seil_sensor_data");
		String sql = "select ";

		for (int i = 0; i < aggregationFormula.length; i++) {
			sql = sql + " " + aggregationFormula[i] + ", ";
		}
		sql = sql + startTS + " as " + timeField + "  from seil_sensor_data group by sensor_id";
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

	public Dataset<Row> fetchDataForAggregation() {
		Dataset<Row> rows = fromTableRows.where(timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.GRANULARITY_IN_SECONDS));
		return rows;
	}

	public double getStartTS() {
		return startTS;
	}


}

package main;

import handlers.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableArchivalAggregation {

	private final MySQLHandler targetMySQLHandler;
	String fromTableName;
	String toTableName;
	double startTS,endTS;
	MySQLHandler sourceMySQLHandler;
	String timeField;
	SparkHandler sparkHandler;
	Dataset<Row> fromTableRows;

	public TableArchivalAggregation(String fromTableName, String toTableName, double endTS) {
		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		this.timeField = "ts";
		this.startTS = -1;
		this.endTS=endTS;
		sourceMySQLHandler = new MySQLHandler(ConfigHandler.SOURCE_MYSQL_HOST, ConfigHandler.SOURCE_MYSQL_USERNAME, ConfigHandler.SOURCE_MYSQL_PASSWORD, ConfigHandler.SOURCE_MYSQL_DATABASE_NAME);
		targetMySQLHandler = new MySQLHandler(ConfigHandler.TARGET_MYSQL_HOST, ConfigHandler.TARGET_MYSQL_USERNAME, ConfigHandler.TARGET_MYSQL_PASSWORD, ConfigHandler.TARGET_MYSQL_DATABASE_NAME);
		this.sparkHandler = new SparkHandler();
		fetchStartTimestamp();
		fromTableRows = sparkHandler.getRowsByTableName(fromTableName);
	}

	public void fetchStartTimestamp() {
		double baseTimestamp = UtilsHandler.tsInSeconds(2016, 10, 1, 0, 0, 0);//base timestamp
		try {
			String sql = "select max(" + timeField + ") from " + toTableName;
			ResultSet resultSet = targetMySQLHandler.query(sql);
			resultSet.next();
			this.startTS = resultSet.getDouble("max(" + timeField + ")");
			if (this.startTS > baseTimestamp) {
				this.startTS += ConfigHandler.GRANULARITY_IN_SECONDS;
			} else {
				resultSet.close();
				//toTableName is empty table then fetch first ts from fromTableName
				sql = "select min(" + timeField + ") from " + fromTableName;
				resultSet = sourceMySQLHandler.query(sql);
				resultSet.next();
				this.startTS = resultSet.getDouble("min(" + timeField + ")");
				if (this.startTS < baseTimestamp) {
					this.startTS = baseTimestamp;
				}
			}
			this.startTS = this.startTS - this.startTS % ConfigHandler.GRANULARITY_IN_SECONDS;
		} catch (SQLException e) {
			LogHandler.logError("[MySQL][Query][TableArchivalAggregation][FetchStartTimestamp]" + e.getMessage());
			UtilsHandler.exit_thread();
		}
	}


	public void startAggregation() {
        /*
        This function starts aggregation of archival data after the last aggregated row present corresponding to this sensor
         */
//		LogHandler.logInfo("[" + fromTableName + "]Aggregation started for minute " + UtilsHandler.tsToStr(this.startTs));
		if (this.startTS >= endTS) {
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
		rows = sparkHandler.sparkSession.sql(sql);
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

	private void storeAggregatedData(Dataset<Row> rows) {
		rows.write().mode(SaveMode.Append).jdbc(ConfigHandler.TARGET_MYSQL_URL, toTableName,
				sparkHandler.getProperties(ConfigHandler.TARGET_MYSQL_USERNAME,ConfigHandler.TARGET_MYSQL_PASSWORD));

	}

	public Dataset<Row> fetchDataForAggregation() {
		Dataset<Row> rows = fromTableRows.where(timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.GRANULARITY_IN_SECONDS));
		return rows;
	}

	public double getStartTS() {
		return startTS;
	}


}

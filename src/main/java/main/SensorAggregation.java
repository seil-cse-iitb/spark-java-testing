package main;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SensorAggregation {

	String fromTableName;
	String sensorId;
	String toTableName;
	double startTS;
	String timeField;
	Spark spark;

	public SensorAggregation(String fromTableName, String sensorId, String toTableName) {
		this.fromTableName = fromTableName;
		this.sensorId = sensorId;
		this.toTableName = toTableName;
		this.timeField = "ts";
		this.startTS = -1;
		this.spark = new Spark();
		fetchStartTimestamp();
	}

	public void fetchStartTimestamp() {
		Dataset<Row> rows = spark.getRowsByTableName(toTableName);
		rows = rows.select(timeField).where(rows.col("sensor_id").equalTo(sensorId)).orderBy(rows.col(timeField).desc()).limit(1);

		if (rows.count() > 0) {
			//if startTS comes from toTableName then we want to aggregate from the next interval(for ex.:next minute);
			Row firstRow = rows.first();
			this.startTS = firstRow.getDouble(firstRow.fieldIndex(timeField));
			this.startTS += ConfigHandler.GRANULARITY_IN_SECONDS;
		} else {
			//toTableName is empty table then fetch first timestamp from fromTableName
			//if startTS comes from fromTableName then we want to aggregate from the same interval(for ex.:same minute);
			rows = spark.getRowsByTableName(fromTableName);
			rows = rows.select(timeField).where("sensor_id = '" + this.sensorId + "'").orderBy(rows.col(timeField).desc()).limit(1);
			if (rows.count() > 0) {
				Row firstRow = rows.first();
				this.startTS = firstRow.getDouble(firstRow.fieldIndex(timeField));
			} else {
				this.startTS = UtilsHandler.tsInSeconds(2016, 10, 1, 0, 0, 0);//base timestamp
			}
		}
		this.startTS = this.startTS - this.startTS % ConfigHandler.GRANULARITY_IN_SECONDS;
	}

	public void startArchivalAggregation() {
        /*
        This function starts aggregation of archival data after the last aggregated row present corresponding to this sensor
         */
		LogHandler.logInfo("[" + sensorId + "]Aggregation started for minute " + UtilsHandler.tsToStr(this.startTS));
//		if (this.startTS > UtilsHandler.tsInSeconds(2018, 2, 26, 0, 0, 0)) {
//			return;
//		}
		Dataset<Row> rows = fetchDataForAggregation();
		if (rows.count() > 0) {
			rows = aggregateDataUsingSQL(rows);
//			rows= aggregateDataUsingDataFrame(rows);
			storeAggregatedData(rows);
		}
		this.goToNextMinute();
	}

	private Dataset<Row> aggregateDataUsingDataFrame(Dataset<Row> rows) {
//		DataFrame
		return null;
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
		DataFrameWriter<Row> df = new DataFrameWriter<Row>(rows);
		df.mode("append").jdbc(ConfigHandler.MYSQL_URL, toTableName, spark.getProperties());
	}

	public Dataset<Row> fetchDataForAggregation() {
		Dataset<Row> rows = spark.getRowsByTableName(fromTableName);
		rows = rows.where("sensor_id= '" + sensorId + "' and " +
				timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.GRANULARITY_IN_SECONDS));
		rows = rows.sort(timeField);
		return rows;
	}

	public double getStartTS() {
		return startTS;
	}


}

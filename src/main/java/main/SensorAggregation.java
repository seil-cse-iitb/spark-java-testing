package main;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SensorAggregation {

	String fromTableName;
	String sensorId;
	String toTableName;
	double startTS;
	MySQLHandler mySQLHandler;
	String timeField;
	Spark spark;
	Dataset<Row> fromTableRows ;

	public SensorAggregation(String fromTableName, String sensorId, String toTableName) {
		this.fromTableName = fromTableName;
		this.sensorId = sensorId;
		this.toTableName = toTableName;
		this.timeField = "ts";
		this.startTS = -1;
		mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
		this.spark = new Spark();
		fetchStartTimestamp();
		fromTableRows = spark.getRowsByTableName(fromTableName);
//		this.startTS = UtilsHandler.tsInSeconds(2017, 10, 3, 0, 0, 0);//base timestamp
	}

	public void fetchStartTimestamp() {
		try {
			String sql = "select " + timeField + " from " + toTableName + " where sensor_id='" + this.sensorId + "' order by " + this.timeField + " desc limit 1";
			ResultSet resultSet = mySQLHandler.query(sql);
//			Dataset<Row> rows = spark.getRowsByTableName(toTableName);
//			Dataset<Row> rows1 = rows.select(timeField).orderBy(rows.col(timeField).desc()).limit(1);
//			Row first = rows1.first();
			if (resultSet.next()) {
				this.startTS = resultSet.getDouble(timeField); //+1 because we want to aggregate from the next second
			} else {
				resultSet.close();
				//toTableName is empty table then fetch first ts from fromTableName
				sql = "select " + timeField + " from " + fromTableName + " where sensor_id='" + this.sensorId + "' order by " + this.timeField + " asc limit 1";
				resultSet = mySQLHandler.query(sql);
				if (resultSet.next()) {
					this.startTS = resultSet.getDouble(timeField); //No +1 because we want to aggregate from this second itself
				} else {
					this.startTS = UtilsHandler.tsInSeconds(2016, 10, 1, 0, 0, 0);//base timestamp
				}
			}
			this.startTS = this.startTS - this.startTS % ConfigHandler.GRANULARITY_IN_SECONDS;
		} catch (SQLException e) {
			LogHandler.logError("[MySQL][Query][SensorClass]" + e.getMessage());
			UtilsHandler.exit_thread();
		}
	}


    public void startArchivalAggregation() {
        /*
        This function starts aggregation of archival data after the last aggregated row present corresponding to this sensor
         */
        LogHandler.logInfo("[" + sensorId + "]Aggregation started for minute " + UtilsHandler.tsToStr(this.startTS));
        if (this.startTS > UtilsHandler.tsInSeconds(2018, 2, 26, 0, 0, 0)) {
            return;
        }
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
        DataFrameWriter<Row> dataFrameWriter = new DataFrameWriter<Row>(rows);
        dataFrameWriter=dataFrameWriter.mode("append");
        dataFrameWriter.jdbc(ConfigHandler.MYSQL_URL, toTableName, spark.getProperties());
    }

	private Dataset<Row> fetchDataForAggregation() {
		Dataset<Row> rows = fromTableRows.where("sensor_id= '" + sensorId + "' and " +
				timeField + " >= " + startTS + " and " + timeField + " < " + (startTS + ConfigHandler.GRANULARITY_IN_SECONDS));
		rows = rows.sort(timeField);
		return rows;
	}

    public double getStartTS() {
        return startTS;
    }


}

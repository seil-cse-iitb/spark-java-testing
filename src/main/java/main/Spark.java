package main;

import org.apache.kerby.config.Conf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class Spark {
    SparkSession sparkSession;
    public Spark() {
        initSession();
        logsOff();
    }

    public void initSession() {
        sparkSession = SparkSession.builder().appName("Java Spark Demo")
                .config("spark.sql.warehouse.dir", "~/Desktop/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("local[4]")
                .getOrCreate();
    }


    public void logsOff(){
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }


    public Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
        properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
        return properties;
    }
    public  Dataset<Row> getRowsByTableName(String tableName){
        Properties properties =getProperties();
        Dataset<Row> rows = this.sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
        return  rows;
    }
}

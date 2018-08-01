package handlers;

import handlers.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkHandler implements scala.Serializable {
    public SparkSession sparkSession;
    JavaSparkContext javaSparkContext;

    public SparkHandler() {
        initSession();
        logsOff();
    }

    public void initSession() {
        sparkSession = SparkSession.builder().appName("Java Spark Demo")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("spark://10.129.149.14:7077") //can't print streaming query on console..Don't know why
//                .master("local[*]")

                .getOrCreate();
    }

    public JavaSparkContext getJavaSparkContext() {
        if (javaSparkContext == null) {
            return new JavaSparkContext(sparkSession.sparkContext());
        } else {
            return javaSparkContext;
        }
    }

    public SQLContext getSQLContext() {
            return this.sparkSession.sqlContext();
    }

    public void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }


    public Properties getProperties(String user, String password) {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return properties;
    }

    public Dataset<Row> getRowsByTableName(String tableName) {
        Properties properties = getProperties(ConfigHandler.SOURCE_MYSQL_USERNAME, ConfigHandler.SOURCE_MYSQL_PASSWORD);
        Dataset<Row> rows = this.sparkSession.read().jdbc(ConfigHandler.SOURCE_MYSQL_URL, tableName, properties);
        return rows;
    }

}

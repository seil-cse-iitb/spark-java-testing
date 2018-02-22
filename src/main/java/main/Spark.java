package main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

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
}

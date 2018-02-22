package main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class Spark {
    SparkConf sparkConf;
    JavaSparkContext javaSparkContext;
    SQLContext sqlContext;

    public Spark() {
        initConfig();
        logginOFF();
    }

    public void initConfig() {
        sparkConf = new SparkConf().setAppName("main.SparkDemo")
                .set("spark.sql.warehouse.dir", "~/Desktop/spark-warehouse")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[4]");
        javaSparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(javaSparkContext);
    }


    public void logginOFF(){
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
}

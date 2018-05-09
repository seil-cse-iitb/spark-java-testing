package main;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import static org.eclipse.paho.client.mqttv3.logging.Logger.CONFIG;

public class IgniteSpark {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.logConf", true)
                .config("spark.submit.deployMode", "cluster")
                .getOrCreate();

        String cfgPath = "";

        Dataset<Row> df = sparkSession.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())              //Data source
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person")     //Table to read.
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
                .load();

        df.createOrReplaceTempView("person");

        Dataset<Row> igniteDF = sparkSession.sql(
                "SELECT * FROM person WHERE name = 'Mary Major'");
    }
}
//                .master("spark://<master IP>:7077")

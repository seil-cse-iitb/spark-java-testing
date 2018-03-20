package test;

import main.LogHandler;
import main.SensorAggregation;
import main.SensorLiveAggregation;
import main.Spark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SensorLiveAggregationTest {

    public static void main(String[] args) {
        final String[] sensorId = {
                "power_k_a",
                "power_k_ch_a",
                "power_k_ch_l",
                "power_k_ch_p1",
                "power_k_ch_p2",
                "power_k_clsrm_ac1",
                "power_k_clsrm_ac2",
                "power_k_clsrm_ac3",
                "power_k_cr_a",
                "power_k_cr_p",
                "power_k_dil_a",
                "power_k_dil_l",
                "power_k_dil_p",
                "power_k_erts_a",
                "power_k_erts_l",
                "power_k_erts_p",
                "power_k_f2_a",
                "power_k_f2_l",
                "power_k_f2_p",
                "power_k_fck_a",
                "power_k_fck_l",
                "power_k_fck_p",
                "power_k_lab_od1",
                "power_k_lab_od2",
                "power_k_lab_od3",
                "power_k_m",
                "power_k_off_a",
                "power_k_off_l",
                "power_k_p",
                "power_k_seil_a",
                "power_k_seil_l",
                "power_k_seil_p",
                "power_k_sr_a",
                "power_k_sr_p",
                "power_k_wc_a",
                "power_k_wc_l",
                "power_k_wc_p",
                "power_k_yc_a",
                "power_k_yc_p",
                "power_lcc_202_l",
                "power_lcc_202_p",
                "power_lcc_23_a",
                "power_lcc_302_l",
                "power_lcc_302_p"
        };

        for (int i = 0; i < 1; i++) {
            final int finalI = i;
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    String toTableName = "live_agg_sch_3";
                    try {
                        LogHandler.logInfo("[Thread][Start] started for sensor_id:" + sensorId[finalI]);
                        SensorLiveAggregation sensorLiveAggregation= new SensorLiveAggregation(sensorId[finalI], toTableName);
                        sensorLiveAggregation.startArchivalAggregation();
                        LogHandler.logInfo("[Thread][End] ended for sensor_id:" + sensorId[finalI]);
                    } catch (Exception e) {
                        LogHandler.logError("From table: sensor_id:[" + sensorId[finalI] + "] To table:[" + toTableName + "]" + e.getMessage());
                    }
                }
            });
            thread.start();
        }
    }


}

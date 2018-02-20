import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class SparkDemo {
    public static void main(String[] args) {
        String brokerUrl = "tcp://10.129.149.9:1883";
        String topic = "data/kresit/sch/4";
        SparkConf sparkConf = new SparkConf().setAppName("SparkDemo")
                .set("spark.sql.warehouse.dir", "~/Desktop/spark-warehouse")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.allowMultipleContexts", "true");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]");
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
        jssc.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK());

        messages.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                System.out.println(stringJavaRDD.toDebugString());
                System.out.println(time.toString());
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("------------------------------Error--------------------------"+e.getMessage());
        }
    }

}

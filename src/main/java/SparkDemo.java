import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import scala.Tuple2;

public class SparkDemo {
    public static void main(String[] args) {
        String brokerUrl = "tcp://10.129.149.9:1883";
        String topic = "data/kresit/sch/4";
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("test")
                .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        jssc.checkpoint("checkpoint");

        JavaDStream<String> lines = MQTTUtils.createStream(jssc, brokerUrl, topic);
        System.out.println(lines.toString());
//        JavaReceiverInputDStream<String> lines = MQTTUtils.createStream(jssc, brokerUrl, topic);
//        JavaReceiverInputDStream<Tuple2<String, String>> lines1 = MQTTUtils.createStream(jssc, brokerUrl, topic);
    }

}

package test;

import main.Sensor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SensorTest {
    Sensor sensor ;
    @Before
    public void init(){
        sensor = new Sensor("sch_3","power_k_sr_a","agg_sch_3");
    }
    @Test
    public void fetchStartTimestampTest(){
        Assert.assertNotEquals(-1,sensor.getStartTS());
    }
    @Test
    public void schemaTest(){
        Dataset<Row> rowDataset = sensor.fetchDataForAggregation();
        StructType schema = rowDataset.schema();
        System.out.println(schema);
    }
}

package test;

import main.SensorAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SensorTest {
    SensorAggregation sensorAggregation;
    @Before
    public void init(){
        sensorAggregation = new SensorAggregation("sch_3","power_k_sr_a","agg_sch_3");
    }
    @Test
    public void fetchStartTimestampTest(){
        Assert.assertNotEquals(-1, sensorAggregation.getStartTS());
    }
    @Test
    public void schemaTest(){
        Dataset<Row> rowDataset = sensorAggregation.fetchDataForAggregation();
        StructType schema = rowDataset.schema();
        System.out.println(schema);
    }
}

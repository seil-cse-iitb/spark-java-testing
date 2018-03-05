package test;

import main.SensorAggregation;
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
}

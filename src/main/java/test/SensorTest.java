package test;

import main.SensorArchivalAggregation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SensorTest {
    SensorArchivalAggregation sensorArchivalAggregation;
    @Before
    public void init(){
        sensorArchivalAggregation = new SensorArchivalAggregation("sch_3","power_k_sr_a","agg_sch_3");
    }
    @Test
    public void fetchStartTimestampTest(){
        Assert.assertNotEquals(-1, sensorArchivalAggregation.getStartTS());
    }
}

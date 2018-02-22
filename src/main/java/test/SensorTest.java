package test;

import main.Sensor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
}

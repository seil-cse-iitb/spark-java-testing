package test;

import main.LogHandler;
import main.Sensor;

public class test {
	public static void main(String[] args) {
		Sensor sensor = new Sensor("sch_3", "power_k_sr_a", "agg_sch_3");
		int i = 2880;
		try {
			while (i > 0) {
				sensor.startArchivalAggregation();

				i--;
			}
		} catch (Exception e) {
			LogHandler.logError("[TEST]" + e.getMessage());
		}
	}
}

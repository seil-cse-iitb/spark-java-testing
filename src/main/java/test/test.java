package test;

import main.Sensor;
import main.UtilsHandler;

import javax.rmi.CORBA.Util;

public class test {
	public static void main(String[] args) {
		Sensor sensor = new Sensor("sch_3","power_k_sr_a","agg_sch_3");
		sensor.startArchivalAggregation();
	}
}

package test;

import main.Sensor;
import main.UtilsHandler;

import javax.rmi.CORBA.Util;

public class test {
	public static void main(String[] args) {
		Sensor sensor = new Sensor("sch_3","power_k_sr_a","agg_sch_3");
		int i=2880;
		while(i>0) {
			sensor.startArchivalAggregation();
			i--;
		}
	}
}

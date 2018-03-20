package test;


import main.LogHandler;
import main.TableArchivalAggregation;

public class TableAggregationTest {
	public static void main(String[] args) {
			Thread thread = new Thread(new Runnable() {
				public void run() {
					String fromTableName = "sch_3";
					String toTableName = "agg_sch_3";
					try {

						LogHandler.logInfo("[Thread][Start] started for table:"+fromTableName );
						TableArchivalAggregation tableArchivalAggregation = new TableArchivalAggregation(fromTableName,toTableName);
						int i = 1051200;
						while (i > 0) {
							tableArchivalAggregation.startAggregation();
							i--;
						}
						LogHandler.logInfo("[Thread][End] ended for table:"+fromTableName);
					} catch (Exception e) {
						LogHandler.logError("From table:[" + fromTableName + "] To table:[" + toTableName + "]" + e.getMessage());
					}
				}
			});
			thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

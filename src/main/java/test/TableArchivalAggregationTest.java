package test;


import handlers.ConfigHandler;
import handlers.LogHandler;
import handlers.UtilsHandler;
import main.TableArchivalAggregation;

public class TableArchivalAggregationTest {
	public static void main(String[] args) {
		LogHandler.logInfo("[SCRIPT_IDENTITY_TEXT : "+ConfigHandler.SCRIPT_IDENTITY_TEXT +"]");
			Thread thread = new Thread(new Runnable() {
				public void run() {
					String fromTableName = "sch_3";
					String toTableName = "agg_sch_3";
					double endTs = UtilsHandler.tsInSeconds(2018, 8, 1, 0, 0, 0);
					try {

						LogHandler.logInfo("[Thread][Start] started for table:"+fromTableName );
						TableArchivalAggregation tableArchivalAggregation = new TableArchivalAggregation(fromTableName,toTableName,endTs);
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

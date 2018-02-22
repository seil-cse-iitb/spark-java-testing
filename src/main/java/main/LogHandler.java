package main;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogHandler {
    public final static String logFilePath = ConfigHandler.LOG_FILE_PATH;
    public static void log(String text){
        System.out.println("[" + UtilsHandler.current_timestamp() + "]" + (text));
        File logFile = new File(LogHandler.logFilePath,"a");
        try {
            FileWriter fw = new FileWriter(logFile);
            fw.append("[" + UtilsHandler.current_timestamp() + "]" + (text) + "\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
            ReportHandler.reportError("[LoggingError]"+e.getMessage());
        }
    }
    public static void logError(String error){
        LogHandler.log("[Error]" + error);
        ReportHandler.reportError(error);
    }
}

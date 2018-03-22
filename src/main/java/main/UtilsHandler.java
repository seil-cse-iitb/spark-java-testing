package main;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class UtilsHandler {
    public final static Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));

    public static String current_timestamp_str(){
        return new Date().toString();
    }
    public static String tsToStr(double timestamp){
        return new Date((long)(timestamp*1000)).toString();
    }
    public static double tsInSeconds(int year,int month,int day,int hour,int min,int sec){
        calendar.set(year,month-1,day,hour,min,sec);
        return calendar.getTime().getTime()/1000;
    }

    public static void exit_thread(){
        System.exit(-1);
        Thread.currentThread().interrupt();
    }
}

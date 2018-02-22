package main;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class UtilsHandler {
    public final static Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));

    public static String current_timestamp(){
        return new Date().toString();
    }
    public static String tsToStr(float timestamp){
        return new Date((long)(timestamp*1000)).toString();
    }
    public static float tsInSeconds(int year,int month,int day,int hour,int min,int sec){
        calendar.set(year,month-1,day,hour,min,sec);
        return calendar.getTimeInMillis(); //TODO Test it properly
    }

    public static void exit_thread(){
        Thread.currentThread().interrupt();
    }
}

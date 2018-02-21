import java.util.Date;

public class UtilsHandler {
    public static String current_timestamp(){
        return new Date().toString();
    }
    public static void exit_thread(){
        Thread.currentThread().interrupt();
    }
}

package main;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ConfigHandler {
    final static String REPORT_RECEIVER_EMAIL = "sapantanted99@gmail.com";
    final static String SCRIPT_IDENTITY_TEXT = "Spark script for aggregation";
    final static String LOG_FILE_PATH = "./log";
    final static String MYSQL_HOST = "10.129.149.11";
//    final static String MYSQL_HOST = "mysql.seil.cse.iitb.ac.in";
    final static String MYSQL_USERNAME = "root";
    final static String MYSQL_PASSWORD = "MySQL@seil";
    final static String MYSQL_DATABASE_NAME = "seil_sensor_data";
    final static String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/" + MYSQL_DATABASE_NAME + "?useSSL=false";
    final static float GRANULARITY_IN_SECONDS = 60;
    final static float DATA_ROWS_PER_SECOND = 1;
    final static StructType SCH_3_SCHEMA = new StructType().add(new StructField("sensor_id",StringType,false,null)).add(new StructField("TS_RECV",DoubleType,false,null)).add(new StructField("srl",IntegerType,true,null)).add(new StructField("TS",DoubleType,true,null)).add(new StructField("VA",DoubleType,true,null)).add(new StructField("W",DoubleType,true,null)).add(new StructField("VAR",DoubleType,true,null)).add(new StructField("PF",DoubleType,true,null)).add(new StructField("VLL",DoubleType,true,null)).add(new StructField("VLN",DoubleType,true,null)).add(new StructField("A",DoubleType,true,null)).add(new StructField("F",DoubleType,true,null)).add(new StructField("VA1",DoubleType,true,null)).add(new StructField("W1",DoubleType,true,null)).add(new StructField("VAR1",DoubleType,true,null)).add(new StructField("PF1",DoubleType,true,null)).add(new StructField("V12",DoubleType,true,null)).add(new StructField("V1",DoubleType,true,null)).add(new StructField("A1",DoubleType,true,null)).add(new StructField("VA2",DoubleType,true,null)).add(new StructField("W2",DoubleType,true,null)).add(new StructField("VAR2",DoubleType,true,null)).add(new StructField("PF2",DoubleType,true,null)).add(new StructField("V23",DoubleType,true,null)).add(new StructField("V2",DoubleType,true,null)).add(new StructField("A2",DoubleType,true,null)).add(new StructField("VA3",DoubleType,true,null)).add(new StructField("W3",DoubleType,true,null)).add(new StructField("VAR3",DoubleType,true,null)).add(new StructField("PF3",DoubleType,true,null)).add(new StructField("V31",DoubleType,true,null)).add(new StructField("V3",DoubleType,true,null)).add(new StructField("A3",DoubleType,true,null)).add(new StructField("FwdVAh",DoubleType,true,null)).add(new StructField("FwdWh",DoubleType,true,null)).add(new StructField("FwdVARhR",DoubleType,true,null)).add(new StructField("FwdVARhC",DoubleType,true,null));
    final static String[] SQL_AGGREGATION_FORMULA_SCH = {"first(sensor_id) as sensor_id","first(srl) as srl","avg(F) as F","avg(W) as W","avg(W1) as W1","avg(W2) as W2","avg(W3) as W3","avg(V1) as V1","avg(V2) as V2","avg(V3) as V3","avg(A) as A","avg(A1) as A1","avg(A2) as A2","avg(A3) as A3","avg(VAR) as VAR","avg(VAR1) as VAR1","avg(VAR2) as VAR2","avg(VAR3) as VAR3","avg(VA) as VA","avg(VA1) as VA1","avg(VA2) as VA2","avg(VA3) as VA3","(avg(W)/avg(VA)) as PF","(avg(W1)/avg(VA1)) as PF1","(avg(W2)/avg(VA2)) as PF2","(avg(W3)/avg(VA3)) as PF3","last(TS_RECV) as TS_RECV","last(FwdWh) as FwdVAh","last(FwdWh) as FwdVARhC","last(FwdWh) as FwdVARhR","last(FwdWh) as FwdWh","(last(FwdWh)-first(FwdWh)) as delta_FwdWh","(count(*)/" + (ConfigHandler.GRANULARITY_IN_SECONDS / ConfigHandler.DATA_ROWS_PER_SECOND)+ "*100) as data_percent",
    };

    final static boolean REPORT_ERROR= true;

	//    final static HashMap<String,String[]> AGGREGATION_FORMULAS = new HashMap();
//	static {
//		AGGREGATION_FORMULAS.put("rish_1",);
//		AGGREGATION_FORMULAS.put("sch_3",);
//		AGGREGATION_FORMULAS.put("temp_5",);
//		AGGREGATION_FORMULAS.put("dht_7",);
//	}

}

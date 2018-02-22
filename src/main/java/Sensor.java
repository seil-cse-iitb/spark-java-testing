import java.sql.ResultSet;
import java.sql.SQLException;

public class Sensor {

    String fromTableName;
    String sensorId;
    String toTableName;
    float startTS;
    MySQLHandler mySQLHandler;
    String timeField;

    public Sensor(String fromTableName, String sensorId, String toTableName) {
        this.fromTableName = fromTableName;
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
        fetchStartTimestamp();

    }

	public String getTimeField() {
		return timeField;
	}

	public void setTimeField(String timeField) {
		this.timeField = timeField;
	}

	public void fetchStartTimestamp() {
        try {
            String sql = "select "+timeField+" from " + toTableName + " order by "+this.timeField+" desc limit 1";
            ResultSet resultSet = mySQLHandler.query(sql);
            if (resultSet.next()) {
                this.startTS = resultSet.getFloat(timeField);
            }else{
            	resultSet.close();
            	//toTableName is empty table then fetch first ts from fromTableName
	            sql ="select "+timeField+" from " + toTableName + " order by "+this.timeField+" asc limit 1";
	            resultSet=mySQLHandler.query(sql);
	            if(resultSet.next()){
	            	this.startTS = resultSet.getFloat(timeField);
	            }else{
					this.startTS = (float)UtilsHandler.tsInSeconds(2016,10,1,0,0,0); //Timestamp of 2016/10/1
	            }
            }
        } catch (SQLException e) {
            LogHandler.logError("[MySQL][Query]" + e.getMessage());
            UtilsHandler.exit_thread();
        }
    }

}

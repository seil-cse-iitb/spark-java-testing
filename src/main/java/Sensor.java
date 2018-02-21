import java.sql.ResultSet;
import java.sql.SQLException;

public class Sensor {

    String fromTableName;
    String sensorId;
    String toTableName;
    float startTS;
    MySQLHandler mySQLHandler;

    public Sensor(String fromTableName, String sensorId, String toTableName) {
        this.fromTableName = fromTableName;
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        mySQLHandler = new MySQLHandler(ConfigHandler.MYSQL_HOST, ConfigHandler.MYSQL_USERNAME, ConfigHandler.MYSQL_PASSWORD, ConfigHandler.MYSQL_DATABASE_NAME);
    }

    public void fetchStartTS() {
        try {
            String query = "select ts from " + toTableName + " order by ts desc limit 1";
            ResultSet resultSet = mySQLHandler.query(query);
            if (resultSet.next()) {
                this.startTS = resultSet.getFloat("ts");
            }
        } catch (SQLException e) {
            LogHandler.logError("[MySQL][Query]" + e.getMessage());
            UtilsHandler.exit_thread();
        }
    }

}

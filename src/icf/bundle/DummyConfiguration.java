package icf.bundle;

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;



public class DummyConfiguration extends AbstractConfiguration{


    private String quoting = "";


    @ConfigurationProperty(order = 1, displayMessageKey = "QUOTING_DISPLAY", helpMessageKey = "QUOTING_HELP")
    public String getQuoting() {
        return this.quoting;
    }

    public void setQuoting(String value) {
        this.quoting = value;
    }

    private String host = "";

    @ConfigurationProperty(order = 2, displayMessageKey = "HOST_DISPLAY", helpMessageKey = "HOST_HELP")
    public String getHost() {
        return this.host;
    }

    public void setHost(String value) {
        this.host = value;
    }

    private String port = "";

    @ConfigurationProperty(order = 3, displayMessageKey = "PORT_DISPLAY", helpMessageKey = "PORT_HELP")
    public String getPort() {
        return this.port;
    }

    public void setPort(String value) {
        this.port = value;
    }

    private String user = "";

    @ConfigurationProperty(order = 4, displayMessageKey = "USER_DISPLAY", helpMessageKey = "USER_HELP")
    public String getUser() {
        return this.user;
    }

    public void setUser(String value) {
        this.user = value;
    }

    private GuardedString password;

    @ConfigurationProperty(order = 5, confidential = true, displayMessageKey = "PASSWORD_DISPLAY", helpMessageKey = "PASSWORD_HELP")
    public GuardedString getPassword() {
        return this.password;
    }

    public void setPassword(GuardedString value) {
        this.password = value;
    }

    private String database = "";

    @ConfigurationProperty(order = 6, displayMessageKey = "DATABASE_DISPLAY", helpMessageKey = "DATABASE_HELP")
    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String value) {
        this.database = value;
    }

    private String table = "";

    @ConfigurationProperty(order = 7, required = true, displayMessageKey = "TABLE_DISPLAY", helpMessageKey = "TABLE_HELP")
    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    private String keyColumn = "";

    @ConfigurationProperty(order = 8, required = true, displayMessageKey = "KEY_COLUMN_DISPLAY", helpMessageKey = "KEY_COLUMN_HELP")
    public String getKeyColumn() {
        return this.keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    private String jdbcDriver = "oracle.jdbc.driver.OracleDriver";

    @ConfigurationProperty(order = 9, displayMessageKey = "JDBC_DRIVER_DISPLAY", helpMessageKey = "JDBC_DRIVER_HELP")
    public String getJdbcDriver() {
        return this.jdbcDriver;
    }

    public void setJdbcDriver(String value) {
        this.jdbcDriver = value;
    }

    private String jdbcUrlTemplate = "jdbc:oracle:thin:@xxx";

    @ConfigurationProperty(order = 10, displayMessageKey = "URL_TEMPLATE_DISPLAY", helpMessageKey = "URL_TEMPLATE_HELP")
    public String getJdbcUrlTemplate() {
        return jdbcUrlTemplate;
    }

    public void setJdbcUrlTemplate(String value) {
        this.jdbcUrlTemplate = value;
    }

	
	@Override
	public void validate() {

		if (StringUtil.isBlank(getTable())) {
			throw new IllegalArgumentException("Table_Is_Blank");
		}

		if (StringUtil.isBlank(getJdbcUrlTemplate())) {
			throw new IllegalArgumentException("Url_Is_Blank");
		}

		if (StringUtil.isNotBlank(getKeyColumn())) {
			throw new IllegalArgumentException("Key_Column_Is_Blank");
		}

		if (getUser() == null) {
			throw new IllegalArgumentException("User_Is_Blank");
		}

		if (getPassword() == null) {
			throw new IllegalArgumentException("Password_Is_Blank");
		}

		if (StringUtil.isBlank(getJdbcDriver())) {
			throw new IllegalArgumentException("JDBC_Driver_Is_Blank");
		}
		try {
			Class.forName(getJdbcDriver());
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("JDBC_Driver_Not_Found");
		}

	}

}

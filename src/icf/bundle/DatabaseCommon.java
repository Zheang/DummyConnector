package icf.bundle;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;

public class DatabaseCommon {
	private Connection conn;
	private DummyConfiguration config;
	//private DatabaseInfo dbInfo;
	private TableInfo tbInfo;

	public DatabaseCommon(DummyConfiguration cfg) {
		this.config = cfg;
		this.tbInfo = new TableInfo(cfg.getTable());
	}

	public Connection getConnection() {
		return this.conn;
	}
	
	public TableInfo getTableInfo(){
		return this.tbInfo;
	}

	public void openConnection() throws SQLException {
		if (conn == null || conn.isClosed()) {
			System.out.println("Get new connection, it is closed");
			this.conn = getNativeConnection();
		}
	}

	private Connection getNativeConnection() {
		Connection connection;
		final String user = config.getUser();
		final GuardedString password = config.getPassword();
		final String driver = config.getJdbcDriver();
		final String url = config.getJdbcUrlTemplate();


		try {
			// load the driver class..
			Class.forName(driver);
			// get the database URL..

			final Properties prop = new Properties();
			prop.put("user", user);
			if (!user.isEmpty()) {
				password.access(new GuardedString.Accessor() {
					public void access(char[] clearChars) {
						prop.put("password", new String(clearChars));
					}
				});
			}

			connection = DriverManager.getConnection(url, prop);
			if (connection.getAutoCommit()) {
				connection.setAutoCommit(false);
			}

		} catch (Exception e) {
			System.out.println("Connection failed");
			throw new ConnectorException(e);
		}

		return connection;
	}

	public void closeConnection() {

		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException expected) {
			// expected
		}

	}
	
	public void rollback(){
		try {
            if (conn != null && !conn.isClosed()) {
                conn.rollback();
                
            }
        } catch (SQLException expected) {
            //expected
        }
	}
	
	public ResultSet excute(String sql) throws SQLException  {
		PreparedStatement stmt = conn.prepareStatement(sql);
		return stmt.executeQuery();
	}
	
	public void commit() throws SQLException {
        conn.commit();
    }
	
	public String sqlGenerator(String action, List<String> para){
		StringBuilder ret = new StringBuilder();
		if(action.equals("create"))
		{
			ret.append("insert into "+ tbInfo.getTableName() + " ");
			StringBuilder sb1 = new StringBuilder(); 
			StringBuilder sb2 = new StringBuilder();
			sb1.append('(');
			sb2.append('(');
			for(int i=0;i<para.size();i=i+2) {
				if(tbInfo.getColumn(para.get(i))!=null)
				{
					sb1.append(para.get(i));
					sb1.append(',');
					sb2.append("'"+para.get(i+1)+ "'");
					sb2.append(',');
				}
				
			}
			sb1.deleteCharAt(sb1.length()-1);
			sb2.deleteCharAt(sb2.length()-1);
			sb1.append(')');
			sb2.append('0');
			ret.append(sb1.toString() + " values " + sb2.toString() +";");
		}
		else if(action.equals("delete"))
		{
			ret.append("delete from "+tbInfo.getTableName()+" where " + para.get(0)+ "='"+para.get(1)+"'");
			
		}
		else if(action.equals("update"))
		{
			ret.append("update "+tbInfo.getTableName()+" set ");
			for(int i=2;i<para.size();i=i+2) {
				if(tbInfo.getColumn(para.get(i))!=null)
				{
					ret.append(para.get(i)+"='"+para.get(i+1)+"'");
					ret.append(',');
					
				}
			}
			ret.deleteCharAt(ret.length()-1);
			ret.append(" where " + para.get(0)+ "='"+para.get(1)+"'");
		}
		else if(action.equals("select"))
		{
			ret.append("select * from "+tbInfo.getTableName()+" ");
						StringBuilder sb= new StringBuilder();
			for(int i=0;i<para.size();i=i+2) {
				if(tbInfo.getColumn(para.get(i))!=null)
				{
					sb.append(para.get(i)+"='"+para.get(i+1)+"'");
					sb.append(',');
					
				}
			}
			if(sb.length()!=0) {
				sb.deleteCharAt(sb.length()-1);
				ret.append("where "+sb.toString());
			}
			
		}
		
		return ret.toString();
		
	}
	
	public void initTableInfo(ResultSet rset) throws SQLException {
		ResultSetMetaData meta = rset.getMetaData();			
        int count = meta.getColumnCount();	
        for (int i = 1; i <= count; i++) {
            String columnName = meta.getColumnName(i);

            int columnSize = meta.getPrecision(i);
            int columnType = meta.getColumnType(i);
            int nullable = meta.isNullable(i);
            boolean writable = meta.isWritable(i);
            boolean readOnly = meta.isReadOnly(i);
            boolean searchable = meta.isSearchable(i);
            boolean isAutoincrement = meta.isAutoIncrement(i);

            ColumnInfo column = new ColumnInfo(columnName, columnSize, columnType, nullable, searchable, writable, readOnly, isAutoincrement);
            if(columnName.equalsIgnoreCase(config.getKeyColumn()))
            	column.setKeyColumn(true);
            tbInfo.addColumn(column);
        }
       
        
	}
	
	public Set<AttributeInfo> getAttrInfos()
	{
		Set<AttributeInfo> rst = new HashSet<AttributeInfo>();
		for(ColumnInfo clInfo : tbInfo.getColumnInfos()){
			AttributeInfoBuilder attrBld = new AttributeInfoBuilder();
			attrBld.setName(clInfo.getColumnName());
			attrBld.setCreateable(false);
			attrBld.setUpdateable(!clInfo.isReadOnly());
			attrBld.setRequired(ResultSetMetaData.columnNoNulls == clInfo.getNullable());
			rst.add(attrBld.build());
		}
		
		return rst;
	}
	

	
	public class TableInfo {
        private String tableName = ""; //name
        
        private String configuredName;

        private Set<ColumnInfo> columnInfos = new HashSet<ColumnInfo>();
        

        public TableInfo(String tableName) {
        	super();
        	this.tableName = tableName;
        }
        


        public String getTableName() {
            return tableName;            
        }
        
        public String getConfiguredName() {
            return configuredName;
        }
        
        public void setConfiguredName(String configuerdName)
        {
        	this.configuredName = configuerdName;
        }
        
        
        public void addColumn(ColumnInfo column) {
        	this.columnInfos.add(column);
        }
        
        public Set<ColumnInfo> getColumnInfos ()
        {
        	return this.columnInfos;
        }
        
        public ColumnInfo getColumn(String columnName) {
        	for(ColumnInfo ci : columnInfos){
        		if(ci.getColumnName().equalsIgnoreCase(columnName))
        		{
        			return ci;
        		}
        	}
        	return null;
        	
        }
        
        public ColumnInfo getKeyColumn() {
        	for(ColumnInfo ci : columnInfos){
        		if(ci.isKeyColumn())
        		{
        			return ci;
        		}
        	}
        	throw new IllegalStateException("KeyColumn not defined");
        	
        }
        

        
	}
	
	public class ColumnInfo {
		final private String columnName;
        final private int columnSize;
        final private int columnType;
        final int nullable;
        final boolean searchable;
        final boolean writable;
        final boolean readOnly;
        final boolean autoincrement;
        
        private boolean keyColumn;

        
        
        public ColumnInfo(String columnName, int columnSize, int columnType, int nullable, boolean searchable, boolean writable, boolean readOnly, boolean autoincrement) {
      
            this.columnName = columnName;
            this.columnSize = columnSize;
            this.columnType = columnType;
            this.nullable = nullable;
            this.searchable = searchable;
            this.writable = writable;
            this.readOnly = readOnly;
            this.autoincrement = autoincrement;
        }
        
        
        public String getColumnName() {
            return columnName;
        }

        public int getColumnType() {
            return columnType;
        }

        public int getColumnSize() {
            return columnSize;
        }

        public int getNullable() {
            return nullable;
        }

        public boolean isSearchable() {
            return searchable;
        }

        public boolean isWritable() {
            return writable;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public boolean isAutoincrement() {
            return autoincrement;
        }
        
        public boolean isKeyColumn() {
            return keyColumn;
        }


        public void setKeyColumn(boolean keyColumn) {
            this.keyColumn = keyColumn;
        }


	}
	


}

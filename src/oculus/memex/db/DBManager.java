package oculus.memex.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import oculus.xdataht.preprocessing.ScriptDBInit;

public class DBManager {
	public String database_type = ScriptDBInit._type;
	public String database_hostname = ScriptDBInit._hostname;
	public String database_port = ScriptDBInit._port;
	public String database_user = ScriptDBInit._user;
	public String database_password = ScriptDBInit._pass;
	public String database_name = ScriptDBInit._oculusSchema;

	public Connection _conn = null;
	public Object _dbLock = new Object();
	public boolean _locked = false;
	public boolean _driverLoaded = false;

	public DBManager(String name, String type, String hostname, String port, String user, String pass) throws Exception {
		database_type = type;
		database_hostname = hostname;
		database_port = port;
		database_user = user;
		database_password = pass;
		database_name = name;

		System.out.println("Logging in to database: (" + name + ", " + type + ", " + hostname + ", " + port + ", " + user + ", " + pass + ")");
		initDriver();
		initDatabase();
	}
	
	private void initDatabase() throws Exception {
		Properties connectionProps = new Properties();
		connectionProps.put("user", database_user);
		connectionProps.put("password", database_password);
		
		Connection conn = DriverManager.getConnection("jdbc:" + database_type + "://" + 
					database_hostname +	":" + database_port + "/", connectionProps);
		
		boolean dbExists = false;
		ResultSet rs = null;
		try {
			// Iterate our set of catalogs (i.e., databases)
			rs = conn.getMetaData().getCatalogs();
			while (rs.next()) {
				if (rs.getString(1).equals(database_name)) {
					dbExists = true;
					break;
				}
			}
		} finally {
			try {
				if (rs != null) { rs.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (!dbExists) {
			tryStatement(conn, "CREATE DATABASE " + database_name);
			conn.close();
		}	
	}
	
	private void initDriver() {
		if (_driverLoaded) return;
		try {
			if ("postgresql".equals(database_type)) {
				Class.forName("org.postgresql.Driver");
			} else {
				Class.forName("com.mysql.jdbc.Driver");
			}
			_driverLoaded = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Connection open() {
		return open(database_name);
	}	
	public Connection open(String db) {
		try {
			synchronized (_dbLock) {
				while (_locked) _dbLock.wait();
				_locked = true;
				_conn = getConnection(db);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return _conn;
	}
	public void close() {
		try {
			synchronized (_dbLock) {
				_locked = false;
				_conn.close();
				_conn = null;
				_dbLock.notify();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Connection getConnection(String db) throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", database_user);
		connectionProps.put("password", database_password);
		conn = DriverManager.getConnection("jdbc:" + database_type + "://" + 
				database_hostname +	":" + database_port + "/" + db, connectionProps);
		return conn;
	}


	public static boolean tryStatement(Connection conn, String sqlStr) {
		boolean success = false;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sqlStr);
			success = true;
		} catch (Exception e) {
			System.out.println("Failed sql: " + sqlStr);
			e.printStackTrace();
			success = false;
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return success;
	}
	
	public static int getInt(Connection conn, String sqlStr, String description) {
		int result = 0;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				result = rs.getInt(1);
			}
		} catch (Exception e) {
			System.out.println("Failed sql (" + description + ") (" + sqlStr + ")");
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public static int getResultCount(Connection conn, String sqlStr, String description) {
		int result = 0;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				result++;
			}
		} catch (Exception e) {
			System.out.println("Failed sql (" + description + ") (" + sqlStr + ")");
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public boolean tableExists(String table) {
		ResultSet tables = null;
		try {
			DatabaseMetaData dbm = _conn.getMetaData();
			tables = dbm.getTables(null, null, table, null);
			return tables.next();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (tables!=null) {
				try {
					tables.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return false; 
	}
	
	public void clearTable(String table) {
		tryStatement(_conn, "TRUNCATE TABLE " + table);
	}

	public Connection getActiveConnection() {
		return _conn;
	}


}

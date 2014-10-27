package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;

public class Keywords {
	private static HashMap<String,String[]> DEFAULT_KEYWORDS = new HashMap<String,String[]>();
	static {
		DEFAULT_KEYWORDS.put("underage", new String[]{
			"baby",
			"fresh",
			"little",
			"young",
			"just started",
			"barely legal",
			"teen",
			"girl"
		});
		DEFAULT_KEYWORDS.put("coersion", new String[]{
			"stable",
			"bottom",
			"daddy",
		});
		DEFAULT_KEYWORDS.put("risky", new String[]{
			"BB",
			"BBBJ",
			"OWO",
			"PSE",
		});
		DEFAULT_KEYWORDS.put("movement", new String[]{
			"newly arrived",
			"just visiting",
			"new in town",
			"fob",
			"fotb",
		});
		DEFAULT_KEYWORDS.put("events", new String[]{
			"super bowl",
			"new year",
			"christmas",
		});
	};
	
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.KEYWORD_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "keyword VARCHAR(45) NOT NULL," +
						  "classifier VARCHAR(45) NOT NULL," +
						  "PRIMARY KEY (id) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (!db.tableExists(TableDB.KEYWORD_TABLE)) {
			System.out.println("Creating table: " + TableDB.KEYWORD_TABLE);
			createTable(db, conn);
			insertDefaultKeywords(conn);
		}
		db.close();
	}
	
	public static void insertDefaultKeywords(Connection conn) {
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TableDB.KEYWORD_TABLE + 
					"(keyword, classifier) VALUES (?,?)");
			int count = 0;
			for (Entry<String,String[]> e:DEFAULT_KEYWORDS.entrySet()) {
				String classifier = e.getKey();
				String[] keywords = e.getValue();
				for (String kw:keywords) {
					pstmt.setString(1, kw);
					pstmt.setString(2, classifier);
					pstmt.addBatch();
					count++;
					if (count % TableDB.BATCH_INSERT_SIZE == 0) {
						pstmt.executeBatch();
					}
				}
			}
			pstmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (pstmt != null) { pstmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
			
			try {
				conn.setAutoCommit(true);
			} catch (SQLException e) {e.printStackTrace();}
		}
	}

	public static HashMap<String,HashSet<String>> getKeywords(TableDB db) {
		Connection conn = db.open();
		HashMap<String,HashSet<String>> result = new HashMap<String,HashSet<String>>();
		String sqlStr = "SELECT keyword,classifier FROM " + TableDB.KEYWORD_TABLE;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String keyword = rs.getString("keyword");
				String classifier = rs.getString("classifier");
				HashSet<String> set = result.get(classifier);
				if (set==null) {
					set = new HashSet<String>();
					result.put(classifier,  set);
				}
				set.add(keyword);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
		}
		db.close();
		return result;
	}		
	
	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin keyword table initialization...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		initTable();

		long end = System.currentTimeMillis();
		System.out.println("Done keyword table initialization in: " + (end-start) + "ms");

	}

}

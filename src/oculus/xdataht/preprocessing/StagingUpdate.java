package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import oculus.xdataht.data.TableDB;

public class StagingUpdate {

	public static String getLatestStaged() {
		String result = null;
		String sqlStr = "select target_table_name from db_process_log where status='complete' AND process_name='ui/main_updater' order by stop_ts DESC limit 1";
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				result = rs.getString("target_table_name");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		db.close();
		return result;
	}
	
	public static String getLatestActive() {
		String result = null;
		String sqlStr = "select target_table_name from db_process_log where status='complete' AND process_name='ui/ads_rename' order by stop_ts DESC limit 1";
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				result = rs.getString("target_table_name");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		db.close();
		return result;
	}
	
	public static void updateStaged() {
		String latestStaged = getLatestStaged();
		String latestActive = getLatestActive();
		System.out.println("Latest staged: " + latestStaged);
		System.out.println("Latest active: " + latestActive);
		if (latestStaged!=null && latestActive!=null && latestStaged.compareTo(latestActive)==0) {
			System.out.println("No change. Done setting active ads table.");
			return;
		}
		if (latestStaged==null) {
			System.out.println("No staged ads table. Done setting active ads table.");
			return;
		}
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String backupName = "ads_backup";
		if (latestActive!=null) backupName = latestActive;
		String sqlStr = "ALTER TABLE ads RENAME TO " + backupName;
		db.tryStatement(conn, sqlStr);
		sqlStr = "ALTER TABLE " + latestStaged + " RENAME TO ads";
		db.tryStatement(conn, sqlStr);
		sqlStr = "INSERT INTO db_process_log  (process_name,version,target_table_name,stop_ts,start_ts,percent_complete,status)" +
				"VALUES ('ui/ads_rename',1,'"+latestStaged+"',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,100,'complete')";
		db.tryStatement(conn, sqlStr);
		db.close();
	}
	
	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin updating staged ads...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		updateStaged();

		long end = System.currentTimeMillis();
		System.out.println("Done activating latest staged ads table in: " + (end-start) + "ms");

	}

}

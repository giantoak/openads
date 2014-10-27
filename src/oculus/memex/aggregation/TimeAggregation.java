package oculus.memex.aggregation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.xdataht.model.TimeVolumeResult;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.TimeLog;

public class TimeAggregation {
	static final public String TIME_TABLE = "temporal";
	private static final int BATCH_INSERT_SIZE = 2000;

	private static void createTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TIME_TABLE+"` (" +
						  "count INT(11) NULL," +
						  "day INT(11) not NULL," +
						  "PRIMARY KEY (day) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void initTable() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TIME_TABLE)) {
			System.out.println("Clearing table: " + TIME_TABLE);
			db.clearTable(TIME_TABLE);
		} else {			
			System.out.println("Creating table: " + TIME_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	private static void computeTimes() {
		HashMap<Long,Integer> timeseries = getTimeSeries();
		ArrayList<TimeVolumeResult> a = new ArrayList<TimeVolumeResult>();
		for (Entry<Long, Integer> r:timeseries.entrySet()) {
			a.add(new TimeVolumeResult(r.getKey(), r.getValue()));
		}
		Collections.sort(a, new Comparator<TimeVolumeResult>() {
			public int compare(TimeVolumeResult o1, TimeVolumeResult o2) {
				return (int)(o1.getDay()-o2.getDay());
			};
		});
		insertTemporalData(a);
	}

	private static void insertTemporalData(ArrayList<TimeVolumeResult> data) {
		PreparedStatement pstmt = null;
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TIME_TABLE + "(day,count) VALUES (?, ?)");
			int count = 0;
			for (TimeVolumeResult r:data) {
				pstmt.setLong(1, r.getDay());
				pstmt.setInt(2, r.getCount());
				pstmt.addBatch();
				count++;
				if (count % BATCH_INSERT_SIZE == 0) {
					pstmt.executeBatch();
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
		db.close();
	}


	private static HashMap<Long, Integer> getTimeSeries() {
		HashMap<Long,Integer> result = new HashMap<Long,Integer>();
		String sqlStr = "SELECT posttime from ads";
		Statement stmt = null;
		MemexHTDB db = MemexHTDB.getInstance();
		Connection conn = db.open();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			Calendar c = Calendar.getInstance();
			while (rs.next()) {
				Date date = rs.getDate("posttime");
				long time = 0;
				if (date!=null) {
					c.setTime(date);
					c.set(Calendar.HOUR,0);
					c.set(Calendar.MINUTE,0);
					c.set(Calendar.SECOND,0);
					c.set(Calendar.MILLISECOND,0);
					time = c.getTimeInMillis()/1000;
				}
				Integer i = result.get(time);
				if (i==null) result.put(time,1);
				else result.put(time,i+1);
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
	
	public static void main(String[] args) {
		TimeLog tl = new TimeLog();
		tl.pushTime("Time aggregation");

		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);

		initTable();
		computeTimes();

		tl.popTime();

	}

}

package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.TimeVolumeResult;

public class TimeAggregation {
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.TIME_TABLE+"` (" +
						  "count INT(11) NULL," +
						  "day INT(11) not NULL," +
						  "PRIMARY KEY (day) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.TIME_TABLE)) {
			System.out.println("Clearing table: " + TableDB.TIME_TABLE);
			db.clearTable(TableDB.TIME_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.TIME_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	private static void computeTimes() {
		TableDB db = TableDB.getInstance();
		HashMap<Long,Integer> timeseries = db.getTimeSeries();
		ArrayList<TimeVolumeResult> a = new ArrayList<TimeVolumeResult>();
		for (Entry<Long, Integer> r:timeseries.entrySet()) {
			a.add(new TimeVolumeResult(r.getKey(), r.getValue()));
		}
		Collections.sort(a, new Comparator<TimeVolumeResult>() {
			public int compare(TimeVolumeResult o1, TimeVolumeResult o2) {
				return (int)(o1.getDay()-o2.getDay());
			};
		});
		db.insertTemporalData(a);
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin time aggregation...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		initTable();
		computeTimes();

		long end = System.currentTimeMillis();
		System.out.println("Done time aggregation in: " + (end-start) + "ms");

	}

}

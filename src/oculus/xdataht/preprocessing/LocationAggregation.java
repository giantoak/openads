package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.geocode.Geocoder;
import oculus.xdataht.model.LocationVolumeResult;
import oculus.xdataht.util.Pair;

public class LocationAggregation {
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.LOCATION_TABLE+"` (" +
						  "location varchar(45) NOT NULL ," +
						  "count INT(11) NULL ," +
						  "lat float(10,6) NULL ," +
						  "lon float(10,6) NULL ," +
						  "PRIMARY KEY (location) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.LOCATION_TABLE)) {
			System.out.println("Clearing table: " + TableDB.LOCATION_TABLE);
			db.clearTable(TableDB.LOCATION_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.LOCATION_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	private static void computeLocations() {
		ArrayList<String> locationColumn = new ArrayList<String>();
		locationColumn.add("location");
		System.out.println("Reading location data...");
		DataTable table = TableDB.getInstance().getDataTableColumns("ads", locationColumn);
		HashMap<String,LocationVolumeResult> resultMap = new HashMap<String,LocationVolumeResult>();
		HashSet<String> notFoundSet = new HashSet<String>();
		System.out.println("Geocoding and aggregating...");
//		int recordcount = 0;
//		int locationcount = 0;
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		for (DataRow row:table.rows) {
//			recordcount++;
			String location = row.get("location");
			if (location==null) continue;
			location = location.toLowerCase().trim();
			LocationVolumeResult r = resultMap.get(location);
			if (r==null) {
				if (notFoundSet.contains(location)) continue;
				try {
//					System.out.print("Geocode " + location);
					Pair<Float,Float> pos = Geocoder.geocode(db, localConn, location);
					if (pos!=null) {
//						locationcount++;
//						System.out.println(" found");
						r = new LocationVolumeResult(1, pos.getFirst(), pos.getSecond(), location);
						resultMap.put(location, r);
//						if (locationcount%100==0) {
//							System.out.println("\tProcessed " + recordcount + " records into " + locationcount + " locations.");
//						}
					} else {
						notFoundSet.add(location);
//						System.out.println(" not found");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				r.setCount(r.getCount()+1);
			}
		}
		db.close();
		ArrayList<LocationVolumeResult> a = new ArrayList<LocationVolumeResult>();
		for (LocationVolumeResult r:resultMap.values()) {
			a.add(r);
		}
		Collections.sort(a, new Comparator<LocationVolumeResult>() {
			public int compare(LocationVolumeResult o1, LocationVolumeResult o2) {
				return o2.getCount()-o1.getCount();
			};
		});
//		for (LocationVolumeResult r:a) {
//			System.out.println(r.getLocation());
//		}
		System.out.println("Inserting aggregation results: " + a.size());
		db.insertLocationData(a);
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin location aggregation...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		initTable();
		computeLocations();

		long end = System.currentTimeMillis();
		System.out.println("Done location aggregation in: " + (end-start) + "ms");

	}

}

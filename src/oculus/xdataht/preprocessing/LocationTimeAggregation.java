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
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.geocode.Geocoder;
import oculus.xdataht.model.LocationTimeVolumeResult;
import oculus.xdataht.model.TimeVolumeResult;
import oculus.xdataht.util.Pair;

public class LocationTimeAggregation {

	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.LOCATION_TIME_TABLE+"` (" +
						  "location varchar(45) NOT NULL," +
						  "count INT(11) NULL," +
						  "lat float(10,6) NULL," +
						  "lon float(10,6) NULL," +
						  "day INT(11) not NULL," +
						  "PRIMARY KEY (location,day) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.LOCATION_TIME_TABLE)) {
			System.out.println("Clearing table: " + TableDB.LOCATION_TIME_TABLE);
			db.clearTable(TableDB.LOCATION_TIME_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.LOCATION_TIME_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	private static void computeLocations() {
		TableDB db = TableDB.getInstance();
		System.out.println("Reading location time series data...");
		HashMap<String,HashMap<Long,Integer>> locations = db.getLocationTimeSeries();
		ArrayList<LocationTimeVolumeResult> locationArray = new ArrayList<LocationTimeVolumeResult>();
		System.out.println("Geocoding and aggregating...");
//		int recordcount = 0;
//		int locationcount = 0;
		Connection localConn = db.open();
		try {
			HashSet<String> notFoundSet = new HashSet<String>();
			for (Entry<String,HashMap<Long, Integer>> timeseries:locations.entrySet()) {
//				recordcount++;
				String location = timeseries.getKey();
				if (location==null) continue;
				if (notFoundSet.contains(location)) continue;
				Pair<Float,Float> pos = Geocoder.geocode(db, localConn, location);
				if (pos!=null) {
//					locationcount++;
					ArrayList<TimeVolumeResult> timeArray = new ArrayList<TimeVolumeResult>();
					for (Entry<Long, Integer> e:timeseries.getValue().entrySet()) {
						timeArray.add(new TimeVolumeResult(e.getKey(), e.getValue()));
					}
					Collections.sort(timeArray, new Comparator<TimeVolumeResult>() {
						public int compare(TimeVolumeResult o1, TimeVolumeResult o2) {
							return (int)(o1.getDay()-o2.getDay());
						};
					});
					locationArray.add(new LocationTimeVolumeResult(location, pos.getFirst(), pos.getSecond(), timeArray));
//					if (locationcount%100==0) {
//						System.out.println("\tProcessed " + recordcount + " records into " + locationcount + " locations.");
//					}
				} else {
					notFoundSet.add(location);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		System.out.println("Inserting location time series data...");
		db.insertLocationTimeData(locationArray);
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin location time aggregation...");
		long start = System.currentTimeMillis();
		ScriptDBInit.initDB(args);
		initTable();
		computeLocations();
		long end = System.currentTimeMillis();
		System.out.println("Done location time aggregation in: " + (end-start) + "ms");
	}

}

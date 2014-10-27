package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.DenseDataTable;
import oculus.xdataht.data.TableDB;

// TODO: Create a table of location -> organization clusterid
public class LocationCluster {
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.LOCATION_CLUSTER_TABLE+"` (" +
						  "location varchar(45) NOT NULL," +
						  "clusterid INT(11) NOT NULL," +
						  "PRIMARY KEY (location,clusterid)," +
						  "KEY clusteridIdx (clusterid)," +
						  "KEY locationIdx (location)" +
						  " )";
			
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.LOCATION_CLUSTER_TABLE)) {
			System.out.println("Clearing table: " + TableDB.LOCATION_CLUSTER_TABLE);
			db.clearTable(TableDB.LOCATION_CLUSTER_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.LOCATION_CLUSTER_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	private static void computeLocations() {
		System.out.println("Reading precluster data...");
		DenseDataTable clusterTable = TableDB.getInstance().getPreclusterColumns();
		clusterTable.updateRowLookup();
		
		ArrayList<String> locationColumn = new ArrayList<String>();
		locationColumn.add("location");
		System.out.println("Reading location data...");
		DataTable table = TableDB.getInstance().getDataTableColumns("ads", locationColumn);
		
		HashMap<String,HashSet<String>> resultMap = new HashMap<String,HashSet<String>>();
		int orgIdx = clusterTable.columns.indexOf("org");
		for (DataRow row:table.rows) {
			String location = row.get("location");
			if (location==null) continue;
			location = location.toLowerCase().trim();
			HashSet<String> r = resultMap.get(location);
			if (r==null) {
				r = new HashSet<String>();
				resultMap.put(location, r);
			}
			String[] preclusterRow = clusterTable.getRowById(row.get("id"));
			if (preclusterRow!=null) r.add(preclusterRow[orgIdx]);
		}

		System.out.println("Inserting aggregation results: " + resultMap.size());
		TableDB.getInstance().insertLocationClusterData(resultMap);
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin location clustering...");
		long start = System.currentTimeMillis();
		ScriptDBInit.initDB(args);
		initTable();
		computeLocations();
		long end = System.currentTimeMillis();
		System.out.println("Done location clustering in: " + (end-start) + "ms");
	}

}

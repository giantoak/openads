package oculus.memex.aggregation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map.Entry;

import oculus.memex.clustering.Cluster;
import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.geo.AdLocations;
import oculus.xdataht.preprocessing.ScriptDBInit;

/**
 *  Create a table of location -> clusterid
 */
public class LocationCluster {
	static final public String LOCATION_CLUSTER_TABLE = "clusters_location";
	public static final int BATCH_INSERT_SIZE = 2000;
	public static final int BATCH_SELECT_SIZE = 50000;

	private static void createTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+LOCATION_CLUSTER_TABLE+"` (" +
						  "location varchar(128) NOT NULL," +
						  "clusterid INT(10) NOT NULL," +
						  "matches INT(10) NOT NULL," +
						  "PRIMARY KEY (location,clusterid)," +
						  "KEY clusteridIdx (clusterid)," +
						  "KEY locationIdx (location)" +
						  " )";
			
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void initTable() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(LOCATION_CLUSTER_TABLE)) {
			System.out.println("Clearing table: " + LOCATION_CLUSTER_TABLE);
			db.clearTable(LOCATION_CLUSTER_TABLE);
		} else {			
			System.out.println("Creating table: " + LOCATION_CLUSTER_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	

	private static HashMap<Integer,Integer> getAdToCluster() {
		HashMap<Integer,Integer> result = new HashMap<Integer,Integer>();
		MemexOculusDB db = MemexOculusDB.getInstance();
		String sqlStr = "SELECT clusterid,ads_id from " + Cluster.CLUSTER_TABLE;
		Statement stmt = null;
		Connection conn = db.open();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer adid = rs.getInt("ads_id");
				Integer clusterid = rs.getInt("clusterid");
				result.put(adid, clusterid);
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
	
	/**
	 * Get a map of location->clusterid->count where the clusters under a location contain 'count' ads at that location.
	 */
	private static HashMap<String,HashMap<Integer,Integer>> getLocationToClusters(HashMap<Integer,Integer> adToCluster) {
		HashMap<String,HashMap<Integer,Integer>> result = new HashMap<String,HashMap<Integer,Integer>>();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Statement stmt = null;
		Connection conn = db.open();
		int maxlocationid = DBManager.getInt(conn, "SELECT max(id) FROM " + AdLocations.AD_LOCATIONS_TABLE, "Get max location id");
		int nextid = 0;
		while (nextid<maxlocationid) {
			String sqlStr = "SELECT ads_id,label from " + AdLocations.AD_LOCATIONS_TABLE +
					" where id>=" + nextid + " and id<=" + (nextid+BATCH_SELECT_SIZE);
			nextid += BATCH_SELECT_SIZE+1;
			try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					Integer adid = rs.getInt("ads_id");
					Integer clusterid = adToCluster.get(adid);
					String label = rs.getString("label");
					if (clusterid!=null) {
						addClusterLocationToMap(result, clusterid, label);
					}
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
		}
		db.close();
		return result;
	}

	public static void addClusterLocationToMap(
			HashMap<String, HashMap<Integer, Integer>> result,
			Integer clusterid, String label) {
		if (result==null) return;
		if (label==null) return;
		HashMap<Integer,Integer> clusters = result.get(label);
		if (clusters==null) {
			clusters = new HashMap<Integer,Integer>();
			result.put(label, clusters);
		}
		Integer count = clusters.get(clusterid);
		if (count==null) {
			clusters.put(clusterid, 1);
		} else {
			clusters.put(clusterid, count+1);
		}
	}
	
	public static void insertLocationClusterData(HashMap<String, HashMap<Integer,Integer>> resultMap) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + LOCATION_CLUSTER_TABLE + "(location,clusterid,matches) VALUES (?,?,?)");
			int count = 0;
			for (Entry<String,HashMap<Integer,Integer>> e:resultMap.entrySet()) {
				String location = e.getKey();
				HashMap<Integer,Integer> clusters = e.getValue();
				for (Integer clusterId:clusters.keySet()) {
					Integer matches = clusters.get(clusterId);
					pstmt.setString(1,location);
					pstmt.setInt(2, clusterId);
					pstmt.setInt(3, matches);
					pstmt.addBatch();
					count++;
					if (count % BATCH_INSERT_SIZE == 0) {
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
		db.close();
	}
	
	
	private static void computeLocations() {
		HashMap<Integer, Integer> adToCluster = getAdToCluster();
		HashMap<String,HashMap<Integer,Integer>> resultMap = getLocationToClusters(adToCluster);

		System.out.println("Inserting aggregation results: " + resultMap.size());
		insertLocationClusterData(resultMap);
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin location clustering...");
		long start = System.currentTimeMillis();
		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		initTable();
		computeLocations();
		long end = System.currentTimeMillis();
		System.out.println("Done location clustering in: " + (end-start) + "ms");
	}

}

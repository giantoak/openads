package oculus.memex.clustering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONArray;

import oculus.memex.aggregation.LocationCluster;
import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.graph.ClusterLinks;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.StringUtil;
import oculus.xdataht.util.TimeLog;


public class Cluster {
	static final public String PROCESSING_PROGRESS_TABLE = "processing_progress";
	public static final String CLUSTER_TABLE = "ads_clusters";
	private static int AD_PROCESS_BATCH_SIZE = 1000;
	private static int BATCH_INSERT_SIZE = 10000;
	private static boolean DEBUG_MODE = false;

	private static void createProgressTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+PROCESSING_PROGRESS_TABLE+"` (" +
						  "id INT NOT NULL AUTO_INCREMENT," +
						  "process_name VARCHAR(45) NOT NULL," +
						  "last_processed INT(10) NOT NULL," +
						  "last_clusterid INT(10) NOT NULL," +
						  "time TIMESTAMP," +
						  "PRIMARY KEY (id) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createClusterTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+CLUSTER_TABLE+"` (" +
						  "id INT NOT NULL AUTO_INCREMENT," +
						  "clusterid INT(10) NOT NULL," +
						  "ads_id INT(10) NOT NULL," +
						  "PRIMARY KEY (id)," + 
						  "KEY ads_idx (ads_id)," +
						  "KEY cluster_idx (clusterid) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Get  (last processed ad, last clusterid) from the processing table.
	 * @return
	 */
	public static Pair<Integer,Integer> getLastID() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		Pair<Integer,Integer> result = new Pair<Integer,Integer>(-1,-1);
		if (!db.tableExists(PROCESSING_PROGRESS_TABLE)) {
			System.out.println("Creating table: " + PROCESSING_PROGRESS_TABLE);
			createProgressTable(db, conn);
		} else {
			String sqlStr = "SELECT last_processed,last_clusterid,max(time) from " + PROCESSING_PROGRESS_TABLE;
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					result.setFirst(rs.getInt(1));
					result.setSecond(rs.getInt(2));
				}
			} catch (Exception e) {
				System.out.println("Failed to get last processed (" + sqlStr + ")");
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
		
	/**
	 * Loop over new ads. Update cluster and cluster_attributes table to put each new ad into a cluster.
	 */
	public static void clusterTable() {
		System.out.println("Clustering...");
		ArrayList<String> clusterAttributes = new ArrayList<String>();
		clusterAttributes.add("phone");
		clusterAttributes.add("email");
		clusterAttributes.add("website");
		clusterAttributes.add("first_id");
		
		// Get the next ad to be processed
		// Find clusters which match phone,email,website
		//    Look at (cluster,attribute,value) table
		// Pick the best match cluster
		// Add the ad to the cluster
		//    Update (ad,cluster) table
		//    Update (cluster,attribute,value) table

		Pair<Integer,Integer> lastIDs = getLastID();
		int lastID = lastIDs.getFirst();
		int nextID = lastID+1;
		int lastCluster = lastIDs.getSecond();
		if (DEBUG_MODE) {
			lastID = 0;
			nextID = 1;
			lastCluster = 0;
		}
		int maxID = MemexAd.getMaxID();
		System.out.println("MAX:" + maxID);
		long start = System.currentTimeMillis();
		
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		ClusterAttributes attributeToClusters = new ClusterAttributes();
		if (!DEBUG_MODE) {
			attributeToClusters.readFromDB(oculusconn);
		}
		ClusterInsertManager cim = new ClusterInsertManager();
		int iterations = 0;
		while (nextID<maxID) {
			HashMap<Integer, MemexAd> adbatch = MemexAd.fetchAdsOculus(htconn, oculusconn, nextID, nextID+AD_PROCESS_BATCH_SIZE-1);
			for (MemexAd ad:adbatch.values()) {
				int clusterid = attributeToClusters.getBestMatch(ad, clusterAttributes);
				if (clusterid==-1) {
					lastCluster++;
					cim.createCluster(oculusdb, oculusconn, lastCluster, ad, clusterAttributes, attributeToClusters);
				} else {
					if (lastCluster<clusterid) lastCluster = clusterid;
					cim.addToCluster(oculusdb, oculusconn, clusterid, ad, clusterAttributes, attributeToClusters);
				}
				if (ad.id>lastID) lastID = ad.id;
			}
			nextID += AD_PROCESS_BATCH_SIZE;
			iterations++;
			if ((iterations%1000)==0) {
				long end = System.currentTimeMillis();
				System.out.println("Processing " + nextID + " of " + maxID + ". Batch time: " + (end-start) + "ms. Last id: " + lastID );
				start = end;
			}
		}

		if (!DEBUG_MODE) {
			cim.batchInsert(oculusconn);
	
			System.out.println("Writing cluster attributes...");
			start = System.currentTimeMillis();
			attributeToClusters.writeToDatabase(oculusdb, oculusconn);
			long end = System.currentTimeMillis();
			System.out.println("Done writing in " + (end-start) + "ms.");
			
			//    Update progress table
			String sqlStr = "insert into " + PROCESSING_PROGRESS_TABLE + "(process_name,last_processed,last_clusterid,time) values ('cluster'," + lastID + "," + lastCluster + ",CURRENT_TIMESTAMP)";
			DBManager.tryStatement(oculusconn, sqlStr);
		}
		oculusdb.close();
		htdb.close();
	}
	
	
	public static HashMap<String,HashSet<String>> getClusterAttributes(MemexOculusDB db, Connection conn, int clusterid) {
		String sqlStr = "select attribute,value from " + ClusterAttributes.CLUSTER_ATTRIBUTE_TABLE + " where clusterid=" + clusterid;

		HashMap<String,HashSet<String>> result = new HashMap<String,HashSet<String>>();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String attribute = rs.getString("attribute");
				String value = rs.getString("value");
				HashSet<String> values = result.get(attribute);
				if (values==null) {
					values = new HashSet<String>();
					result.put(attribute, values);
				}
				values.add(value);
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
		return result;
	}
	
	public static void getAdsInClusters(JSONArray clusterids, HashMap<Integer,HashSet<Integer>> clusterAds, int limit) {
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();		
		for(int i = 0;i<clusterids.length();i++) {
			Statement stmt = null;
			try {
				int clusterid = clusterids.getInt(i);
				String sqlStr = "SELECT ads_id FROM " + CLUSTER_TABLE + 
						" where clusterid="+clusterid+(limit>0?" limit 0,"+limit:"");
				HashSet<Integer> ads = new HashSet<Integer>();
				stmt = oculusconn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					int adid = rs.getInt("ads_id");
					ads.add(adid);
				}
				clusterAds.put(clusterid, ads);
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
		oculusdb.close();
	}
	
	public static void getAdsInCluster(int clusterid, HashSet<Integer> ads, int limit) {
		String sqlStr = "SELECT ads_id FROM " + CLUSTER_TABLE + " where clusterid="+clusterid+(limit>0?" limit 0,"+limit:"");
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		Statement stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				ads.add(adid);
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
		oculusdb.close();
	}
	
	public static void getAdsInClusters(HashSet<Integer> clusterids, HashSet<Integer> ads) {
		if (clusterids.size()==0) return;
		String sqlStr = "SELECT ads_id FROM " + CLUSTER_TABLE + " where clusterid IN "+StringUtil.hashSetToSqlList(clusterids);
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		Statement stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				ads.add(adid);
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
		oculusdb.close();
	}
	
	public static void deleteClusters(HashSet<Integer> clusterids) {
		if (clusterids.size()==0) return;
		String clusterStr = StringUtil.hashSetToSqlList(clusterids);
		String sqlStr = "DELETE FROM " + CLUSTER_TABLE + " where clusterid IN " + clusterStr;
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		DBManager.tryStatement(oculusconn, sqlStr);
		
		deleteClusterDetails(oculusconn, clusterStr);

		oculusdb.close();
	}

	private static void deleteClusterDetails(Connection oculusconn, String clusterStr) {
		String sqlStr;
		sqlStr = "DELETE FROM " + ClusterAttributes.CLUSTER_ATTRIBUTE_TABLE + " where clusterid IN " + clusterStr;
		DBManager.tryStatement(oculusconn, sqlStr);
		
		sqlStr = "DELETE FROM " + ClusterDetails.CLUSTER_DETAILS_TABLE + " where clusterid IN " + clusterStr;
		DBManager.tryStatement(oculusconn, sqlStr);
		
		sqlStr = "DELETE FROM " + ClusterLinks.CLUSTER_LINKS_TABLE + " where clusterid IN " + clusterStr + " or otherid IN " + clusterStr;
		DBManager.tryStatement(oculusconn, sqlStr);
		
		sqlStr = "DELETE FROM " + LocationCluster.LOCATION_CLUSTER_TABLE + " where clusterid IN " + clusterStr;
		DBManager.tryStatement(oculusconn, sqlStr);
	}
	
	private static class ClusterInsertManager {
		ArrayList<Pair<Integer,Integer>> clustersToInsert = new ArrayList<Pair<Integer,Integer>>();
		
		/**
		 * Add the ad to the cluster table and any new cluster attributes to cluster_attributes.
		 */
		void addToCluster(MemexOculusDB db, Connection conn, int clusterid, MemexAd ad, ArrayList<String> clusterAttributes, ClusterAttributes attributeToClusters) {
			clustersToInsert.add(new Pair<Integer,Integer>(ad.id,clusterid));
			if (clustersToInsert.size()>BATCH_INSERT_SIZE) {
				batchInsert(conn);
			}
			for (String attribute:clusterAttributes) {
				HashSet<String> newValues = ad.attributes.get(attribute);
				if (newValues!=null) {
					for (String value:newValues) {
						attributeToClusters.addValue(clusterid, attribute, value, 1);
					}
				}
			}
		}

		/**
		 * Insert the (ads_id,clusterid) into the cluster table and the attributes into the cluster_attributes table.
		 */
		void createCluster(MemexOculusDB db, Connection conn, int clusterid, MemexAd ad, ArrayList<String> clusterAttributes, ClusterAttributes attributeToClusters) {
			clustersToInsert.add(new Pair<Integer,Integer>(ad.id,clusterid));
			if (clustersToInsert.size()>BATCH_INSERT_SIZE) {
				batchInsert(conn);
			}
			attributeToClusters.insertValues(clusterid, ad.attributes, clusterAttributes);
		}

		void batchInsert(Connection conn) {
			if (DEBUG_MODE) return;
			if (clustersToInsert.size()>0) {
				PreparedStatement pstmt = null;
				try {
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement("insert into " + CLUSTER_TABLE + "(ads_id,clusterid) values (?,?)");
					for (Pair<Integer,Integer> entry:clustersToInsert) {
						pstmt.setInt(1,entry.getFirst());
						pstmt.setInt(2,entry.getSecond());
						pstmt.addBatch();
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
			clustersToInsert.clear();
		}
		
	}

	
	public static void precomputeClusters() {
		System.out.println("Beginning clustering phases");

		if (!DEBUG_MODE) {
			MemexOculusDB db = MemexOculusDB.getInstance();
			Connection conn = db.open(ScriptDBInit._oculusSchema);
			if (!db.tableExists(CLUSTER_TABLE)) {
				System.out.println("Creating table: " + CLUSTER_TABLE);
				createClusterTable(db, conn);
			}
			ClusterAttributes.initTable(db, conn);
			db.close();
		}
		
		try {
			System.out.println("Clustering:  ");
			long start = System.currentTimeMillis();
			clusterTable();
			long end = System.currentTimeMillis();
			System.out.println("Done clustering in: " + (end-start) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static HashMap<String,Integer> getSimpleClusterCounts(HashSet<Integer> matchingAds) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT clusterid,count(*) as matching from " + CLUSTER_TABLE + " where ads_id IN (";
		boolean isFirst = true;
		for (Integer adid:matchingAds) {
			if (isFirst) isFirst = false;
			else sqlStr += ",";
			sqlStr += adid;
		}
		sqlStr += ") group by clusterid";
		Statement stmt = null;
		HashMap<String,Integer> result = new HashMap<String,Integer>();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String clusterid = rs.getString("clusterid");
				Integer matching = rs.getInt("matching");
				result.put(clusterid, matching);
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
	
	public static HashSet<Integer> getSimpleClusters(HashSet<Integer> matchingAds) {
		HashSet<Integer> result = new HashSet<Integer>();
		if (matchingAds.size()==0) return result;
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT distinct clusterid from " + CLUSTER_TABLE + " where ads_id IN " + StringUtil.hashSetToSqlList(matchingAds);

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int clusterid = rs.getInt("clusterid");
				result.add(clusterid);
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
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin preclustering...");
		long start = System.currentTimeMillis();

		ScriptDBInit.readArgs(args);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		precomputeClusters();

		long end = System.currentTimeMillis();
		System.out.println("Done preclustering in: " + (end-start) + "ms");

	}

	public static HashSet<Integer> updateClusters(HashSet<Integer> matchingAds, ClusterAttributes attributeToClusters, TimeLog tl) {
		// Fetch clusterids containing the ads
		HashSet<Integer> matchingClusters = getSimpleClusters(matchingAds);
		HashSet<Integer> alteredClusters = new HashSet<Integer>();

		if (matchingAds==null||matchingAds.size()==0) return alteredClusters;
		
		// Expand the list of ads to include all those in the affected clusters
		getAdsInClusters(matchingClusters, matchingAds);

		deleteClusters(matchingClusters);

		tl.pushTime("Read attributes");
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		attributeToClusters.readFromDB(oculusconn);
		oculusdb.close();
		tl.popTime();
		
		
		ArrayList<String> clusterAttributes = new ArrayList<String>();
		clusterAttributes.add("phone");
		clusterAttributes.add("email");
		clusterAttributes.add("website");
		clusterAttributes.add("first_id");
		
		Pair<Integer,Integer> lastIDs = getLastID();

		int lastID = lastIDs.getFirst();
		int lastCluster = lastIDs.getSecond();

		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		oculusconn = oculusdb.open();
		ClusterInsertManager cim = new ClusterInsertManager();

		HashMap<Integer, MemexAd> adbatch = MemexAd.fetchAdsOculus(htconn, oculusconn, matchingAds);

		for (MemexAd ad:adbatch.values()) {
			int clusterid = attributeToClusters.getBestMatch(ad, clusterAttributes);
			if (clusterid==-1) {
				lastCluster++;
				clusterid = lastCluster;
				cim.createCluster(oculusdb, oculusconn, clusterid, ad, clusterAttributes, attributeToClusters);
			} else {
				if (lastCluster<clusterid) lastCluster = clusterid;
				cim.addToCluster(oculusdb, oculusconn, clusterid, ad, clusterAttributes, attributeToClusters);
			}
			alteredClusters.add(clusterid);
		}

		cim.batchInsert(oculusconn);

		String clusterStr = StringUtil.hashSetToSqlList(alteredClusters);
		deleteClusterDetails(oculusconn, clusterStr);
		
		attributeToClusters.writeSubset(oculusdb, oculusconn, alteredClusters);
			
		//    Update progress table
		String sqlStr = "insert into " + PROCESSING_PROGRESS_TABLE + "(process_name,last_processed,last_clusterid,time) values ('cluster'," + lastID + "," + lastCluster + ",CURRENT_TIMESTAMP)";
		DBManager.tryStatement(oculusconn, sqlStr);

		oculusdb.close();
		htdb.close();
		
		return alteredClusters;
	}
}

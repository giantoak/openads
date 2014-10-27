package oculus.xdataht.preprocessing;

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
import java.util.Map;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.Pair;

/**
 * Create a table of organization clusterid -> related clusterid, link column, link value
 * 
 */
public class ClusterLinks {

	private static class LinkData {
		String clusterid;
		String attribute;
		String value;
		public LinkData(String clusterid, String attribute, String value) {
			this.clusterid = clusterid;
			this.attribute = attribute;
			this.value = value;
		}
		
	}
	
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.CLUSTER_LINKS_TABLE+"` (" +
						  "linkid INT(11) NOT NULL AUTO_INCREMENT," +
						  "clusterid INT(11) NOT NULL," +
						  "otherid INT(11) NOT NULL," +
						  "attribute TEXT," +
						  "value TEXT," +
						  "PRIMARY KEY (linkid) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.CLUSTER_LINKS_TABLE)) {
			System.out.println("Clearing table: " + TableDB.CLUSTER_LINKS_TABLE);
			db.clearTable(TableDB.CLUSTER_LINKS_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.CLUSTER_LINKS_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	
	/**
	 * Extract field values from the comma separated fieldStr.
	 * Put id->values in reverse 
	 * Update value->id in map
	 */
	public static void updateMap(String id, String fieldStr, HashMap<String,HashSet<String>> map, HashMap<String,HashSet<String>> reverse) {
		if (fieldStr==null || fieldStr.length()==0) return;
		String[] values = fieldStr.split(",");
		HashSet<String> fieldList = reverse.get(id);
		for (String v:values) {
			if (v==null||v.length()==0) continue;
			HashSet<String> idList = map.get(v);
			if (idList==null) {
				idList = new HashSet<String>();
				map.put(v, idList);
			}
			idList.add(id);
			if (fieldList==null) {
				fieldList = new HashSet<String>();
				reverse.put(id, fieldList);
			}
			fieldList.add(v);
		}
	}

	
	public static HashMap<String,ArrayList<LinkData>> getPreclusterAggregation(TableDB db) {
		Connection conn = db.open();
		HashMap<String,ArrayList<LinkData>> result = new HashMap<String,ArrayList<LinkData>>();
		String sqlStr = "SELECT ads.id,phone_numbers,email,websites,"
				+ "precluster.org_cluster_id AS org "
				+ "FROM ads INNER JOIN precluster ON ads.id=precluster.ad_id";

		// Create maps of clusterid->values and value->clusterids
		HashMap<String,HashSet<String>> phoneToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> emailToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> websiteToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToPhone = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToEmail = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToWebsite = new HashMap<String,HashSet<String>>();
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String org = rs.getString("org");
				updateMap(org, rs.getString("phone_numbers"), phoneToCluster, clusterToPhone);
				updateMap(org, rs.getString("email"), emailToCluster, clusterToEmail);
				updateMap(org, rs.getString("websites"), websiteToCluster, clusterToWebsite);
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
		
		createLinks("phone", result, phoneToCluster, clusterToPhone);
		createLinks("email", result, emailToCluster, clusterToEmail);
		createLinks("websites", result, websiteToCluster, clusterToWebsite);
		
		return result;
	}

	public static HashMap<String,ArrayList<LinkData>> getPreclusterAggregationTest(TableDB db) {
		Connection conn = db.open();
		HashMap<String,ArrayList<LinkData>> result = new HashMap<String,ArrayList<LinkData>>();
		String sqlStr = "SELECT ads.id,phone_numbers,email,websites,"
				+ "precluster.org_cluster_id AS org "
				+ "FROM ads INNER JOIN precluster ON ads.id=precluster.ad_id "
				+ "WHERE ads.phone_numbers LIKE '%6263449068%'";

		// Create maps of clusterid->values and value->clusterids
		HashMap<String,HashSet<String>> phoneToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> emailToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> websiteToCluster = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToPhone = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToEmail = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToWebsite = new HashMap<String,HashSet<String>>();
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String org = rs.getString("org");
				updateMap(org, rs.getString("phone_numbers"), phoneToCluster, clusterToPhone);
				updateMap(org, rs.getString("email"), emailToCluster, clusterToEmail);
				updateMap(org, rs.getString("websites"), websiteToCluster, clusterToWebsite);
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
		
		createLinks("phone", result, phoneToCluster, clusterToPhone);
		createLinks("email", result, emailToCluster, clusterToEmail);
		createLinks("websites", result, websiteToCluster, clusterToWebsite);
		
		return result;
	}

	public static void printLinks(HashMap<String,ArrayList<LinkData>> linkMap) {
		System.out.println("Outputing links:");
		for (Entry<String,ArrayList<LinkData>> e:linkMap.entrySet()) {
			String clusterId = e.getKey();
			for (LinkData ld:e.getValue()) {
				System.out.println("\t" + clusterId + "," + ld.clusterid+ "," + ld.attribute + "," + ld.value);
			}
		}
	}
	
	/**
	 * Given (clusterid->field values) and (field value->clusterids) 
	 * create a list of (clusterid1,clusterid2,commonvalue) links.
	 */
	private static void createLinks(String column, HashMap<String, ArrayList<LinkData>> result,
			HashMap<String, HashSet<String>> fieldToCluster,
			HashMap<String, HashSet<String>> clusterToField) {
		for (Map.Entry<String,HashSet<String>> entry:clusterToField.entrySet()) {
			String clusterid = entry.getKey();
			ArrayList<LinkData> links = result.get(clusterid);
			for (String field:entry.getValue()) {
				HashSet<String> related = fieldToCluster.get(field);
				for (String otherid:related) {
					// Make links only one direction
					if (Integer.parseInt(otherid)<Integer.parseInt(clusterid)) {
						if (links==null) {
							links = new ArrayList<LinkData>();
							result.put(clusterid, links);
						}
						links.add(new LinkData(otherid, column, field));
					}
				}
			}
		}
	}
	
	public static void insertClusterLinks(TableDB db, HashMap<String,ArrayList<LinkData>> linkMap) {
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TableDB.CLUSTER_LINKS_TABLE + 
					"(clusterid, otherid, attribute, value) VALUES (?,?,?,?)");
			int count = 0;
			for (Entry<String,ArrayList<LinkData>> e:linkMap.entrySet()) {
				String clusterId = e.getKey();
				for (LinkData ld:e.getValue()) {
					pstmt.setString(1, clusterId);
					pstmt.setString(2, ld.clusterid);
					pstmt.setString(3, ld.attribute);
					pstmt.setString(4, ld.value);
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
		db.close();
	}

	private static void computeLinks() {
		System.out.println("Reading precluster data...");
		TableDB db = TableDB.getInstance();
//		HashMap<String,ArrayList<LinkData>> linkTable = getPreclusterAggregationTest(db);
//		printLinks(linkTable);
		HashMap<String,ArrayList<LinkData>> linkTable = getPreclusterAggregation(db);
		
		initTable();
		System.out.println("Writing cluster links...");
		insertClusterLinks(db, linkTable);
	}

	/**
	 * Given a list of cluster ids, fetch related clusters and their link reasons. 
	 * Triples are (otherid, shared attribute, shared value).
	 */
	public static HashMap<String,HashMap<String,Pair<String,String>>> getLinks(HashSet<String> clusterids) {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		HashMap<String,HashMap<String,Pair<String,String>>> result = new HashMap<String,HashMap<String,Pair<String,String>>>();
		String clusterList = "";
		boolean isFirst = true;
		for (String clusterid:clusterids) {
			if (isFirst) isFirst = false;
			else clusterList += ",";
			clusterList += clusterid;
		}
		String sqlStr = "SELECT clusterid,otherid,attribute,value from " + TableDB.CLUSTER_LINKS_TABLE
				+ " WHERE clusterid IN (" + clusterList + ") OR otherid IN (" + clusterList + ")";
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String clusterid = rs.getString("clusterid");
				String otherid = rs.getString("otherid");
				String attribute = rs.getString("attribute");
				String value = rs.getString("value");

				// Add the link (clusterid->otherid)
				HashMap<String,Pair<String,String>> links = result.get(clusterid);
				if (links==null) {
					links = new HashMap<String,Pair<String,String>>();
					result.put(clusterid, links);
				}
				links.put(otherid, new Pair<String,String>(attribute, value));

				// Add the reverse link (otherid->clusterid)
				links = result.get(otherid);
				if (links==null) {
					links = new HashMap<String,Pair<String,String>>();
					result.put(otherid, links);
				}
				links.put(clusterid, new Pair<String,String>(attribute, value));
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

	public static void mergeClusters() {

		System.out.println("Read clusters...");
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT clusterid,otherid from " + TableDB.CLUSTER_LINKS_TABLE;
		HashMap<String,HashSet<String>> allLinks = new HashMap<String,HashSet<String>>();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String id1 = rs.getString("clusterid");
				String id2 = rs.getString("otherid");
				HashSet<String> set = allLinks.get(id1);
				if (set==null) {
					set = new HashSet<String>();
					allLinks.put(id1, set);
				}
				set.add(id2);
				set = allLinks.get(id2);
				if (set==null) {
					set = new HashSet<String>();
					allLinks.put(id2, set);
				}
				set.add(id1);
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

		System.out.println("Merge " + allLinks.size() + " clusters...");

		int maxSize = 0;
		int totalSize = 0;
		
		// Remove items from all links while creating connected sets in toMerge
		HashMap<String,HashSet<String>> toMerge = new HashMap<String,HashSet<String>>();
		while (allLinks.size()>0) {
			String clusterid = allLinks.keySet().iterator().next();
			HashSet<String> children = allLinks.remove(clusterid);
			toMerge.put(clusterid, children);
			@SuppressWarnings("unchecked")
			HashSet<String> toExplore = (HashSet<String>)children.clone();
			while (toExplore.size()>0) {
				String childid = toExplore.iterator().next();
				toExplore.remove(childid);
				HashSet<String> subChildren = allLinks.remove(childid);
				if (subChildren!=null) {
					for (String subChild:subChildren) {
						if (!children.contains(subChild)) {
							children.add(subChild);
							toExplore.add(subChild);
						}
					}
				}
			}
			if (children.size()>maxSize) {
				maxSize = children.size();
				System.out.println("Max Size: " + maxSize);
			}
			totalSize += children.size();
		}
		System.out.println("Average Size: " + (totalSize/toMerge.size()));
		System.out.println("Total Clusters: " + toMerge.size());
		db.close();
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin cluster links calculation...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		computeLinks();
//		mergeClusters();

		long end = System.currentTimeMillis();
		System.out.println("Done location cluster links calculation in: " + (end-start) + "ms");

	}

}

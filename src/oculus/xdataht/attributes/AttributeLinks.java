package oculus.xdataht.attributes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;

/**
 * Create a table of attributeid -> related attributeid, matching ads
 * 
 */
public class AttributeLinks {
	public static String ATTRIBUTES_LINKS_TABLE = "attributes_links";

	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTES_LINKS_TABLE+"` (" +
						  "linkid INT(11) NOT NULL AUTO_INCREMENT," +
						  "attributes_id INT(11) NOT NULL," +
						  "otherid INT(11) NOT NULL," +
						  "count INT(11) NOT NULL," +
						  "PRIMARY KEY (linkid) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(ATTRIBUTES_LINKS_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTES_LINKS_TABLE);
			db.clearTable(ATTRIBUTES_LINKS_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTES_LINKS_TABLE);
			createTable(db, conn);
		}
		db.close();
	}
	
	public static void writeAttributeLinks(HashMap<Integer, HashMap<Integer, Integer>> links) {
		initTable();
		
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTES_LINKS_TABLE + 
					"(attributes_id,otherid,count) VALUES (?,?,?)");
			int count = 0;
			for (Entry<Integer,HashMap<Integer,Integer>> e:links.entrySet()) {
				Integer attrid = e.getKey();
				HashMap<Integer,Integer> linkData = e.getValue();
				for (Entry<Integer,Integer> linkDatum:linkData.entrySet()) {
					pstmt.setInt(1, attrid);
					pstmt.setInt(2, linkDatum.getKey());
					pstmt.setInt(3, linkDatum.getValue());
					pstmt.addBatch();
					count++;
					if (count % TableDB.BATCH_INSERT_SIZE == 0) {
						pstmt.executeBatch();
					}
				}
			}
			pstmt.executeBatch();
			System.out.println("Inserted " + count + " attribute links");
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

	/**
	 * Given a list of attribute values, fetch related attributes and their shared ad count. 
	 * Triples are (attribute1, attribute2, count).
	 */
	public static HashMap<Integer, HashMap<Integer, Integer>> getLinks(HashSet<Integer> matchingClusters) {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		HashMap<Integer,HashMap<Integer,Integer>> result = new HashMap<Integer,HashMap<Integer,Integer>>();
		String valueList = "";
		boolean isFirst = true;
		for (Integer value:matchingClusters) {
			if (isFirst) isFirst = false;
			else valueList += ",";
			valueList += "'" + value + "'";
		}
		String sqlStr = "SELECT attributes_id,otherid,count from " + ATTRIBUTES_LINKS_TABLE	+ 
				" WHERE attributes_id IN (" + valueList + ") or " +
				"otherid IN (" + valueList + ")";
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer id1 = rs.getInt("attributes_id");
				Integer id2 = rs.getInt("otherid");
				Integer count = rs.getInt("count");

				// Add the link (id1->id2)
				HashMap<Integer,Integer> links = result.get(id1);
				if (links==null) {
					links = new HashMap<Integer,Integer>();
					result.put(id1, links);
				}
				links.put(id2, count);

				// Add the reverse link (id2->id1)
				links = result.get(id2);
				if (links==null) {
					links = new HashMap<Integer,Integer>();
					result.put(id2, links);
				}
				links.put(id1, count);
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

	

}

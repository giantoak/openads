package oculus.memex.graph;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import oculus.memex.clustering.AttributeValue;
import oculus.memex.clustering.MemexAd;
import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.StringUtil;
import oculus.xdataht.util.TimeLog;

/**
 * Create a table of attribute -> related attribute, adcount
 * 
 */
public class AttributeLinks {
	static final public String ATTRIBUTES_TABLE = "attributes";
	static final public String ATTRIBUTES_LINKS_TABLE = "attributes_links";
	public static final int BATCH_INSERT_SIZE = 2000;
	private static int AD_PROCESS_BATCH_SIZE = 1000;
	private static final int WHERE_IN_SELECT_SIZE = 2000;

	private static void createTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTES_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "attribute VARCHAR(32) NOT NULL," +
						  "value VARCHAR(2500) NOT NULL," +
						  "PRIMARY KEY (id) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createLinksTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTES_LINKS_TABLE+"` (" +
						  "linkid INT(11) NOT NULL AUTO_INCREMENT," +
						  "attr1 VARCHAR(32) NOT NULL," +
						  "attr2 VARCHAR(32) NOT NULL," +
						  "val1 VARCHAR(2500)," +
						  "val2 VARCHAR(2500)," +
						  "count INT(11)," +
						  "PRIMARY KEY (linkid) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void initTables() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(ATTRIBUTES_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTES_TABLE);
			db.clearTable(ATTRIBUTES_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTES_TABLE);
			createTable(db, conn);
		}
		if (db.tableExists(ATTRIBUTES_LINKS_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTES_LINKS_TABLE);
			db.clearTable(ATTRIBUTES_LINKS_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTES_LINKS_TABLE);
			createLinksTable(db, conn);
		}
		db.close();
	}
	
	public static void insertAttributes(HashMap<AttributeValue, HashMap<AttributeValue, Integer>> linkTable) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTES_TABLE + "(attribute,value) VALUES (?,?)");
			int count = 0;
			for (Entry<AttributeValue, HashMap<AttributeValue, Integer>> e:linkTable.entrySet()) {
				AttributeValue av = e.getKey();
				pstmt.setString(1, av.attribute);
				pstmt.setString(2, av.value);
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

	
	public static void insertAttributeLinks(HashMap<AttributeValue, HashMap<AttributeValue, Integer>> linkTable) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTES_LINKS_TABLE + 
					"(attr1,attr2,val1,val2,count) VALUES (?,?,?,?,?)");
			int count = 0;
			for (Entry<AttributeValue, HashMap<AttributeValue, Integer>> e:linkTable.entrySet()) {
				AttributeValue val1 = e.getKey();
				for (Entry<AttributeValue,Integer> e2:e.getValue().entrySet()) {
					AttributeValue val2 = e2.getKey();
					Integer linkCount = e2.getValue();
					pstmt.setString(1, val1.attribute);
					pstmt.setString(2, val2.attribute);
					pstmt.setString(3, val1.value);
					pstmt.setString(4, val2.value);
					pstmt.setInt(5, linkCount);
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

	/**
	 * Loop over ads.
	 */
	private static HashMap<AttributeValue,HashMap<AttributeValue,Integer>> getAttributeLinks() {
		HashMap<AttributeValue,HashMap<AttributeValue,Integer>> result = new HashMap<AttributeValue,HashMap<AttributeValue,Integer>>();

		int lastID = -1;
		int nextID = 0;
		int maxID = MemexAd.getMaxID();
		System.out.println("MAX:" + maxID);
		long start = System.currentTimeMillis();
		
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		int iterations = 0;
		int totalLinks = 0;
		while (nextID<maxID) {
			HashMap<Integer, MemexAd> adbatch = MemexAd.fetchAdsOculus(htconn, oculusconn, nextID, nextID+AD_PROCESS_BATCH_SIZE, false);
			for (MemexAd ad:adbatch.values()) {
				if (ad.id>lastID) lastID = ad.id;
				for (Entry<String,HashSet<String>> e:ad.attributes.entrySet()) {
					String attribute = e.getKey();
					for (String value:e.getValue()) {
						AttributeValue av = new AttributeValue(attribute,value.toLowerCase());
						HashMap<AttributeValue, Integer> linkedAttrs = result.get(av);
						if (linkedAttrs==null) {
							linkedAttrs = new HashMap<AttributeValue,Integer>();
							result.put(av, linkedAttrs);
						}
						for (Entry<String,HashSet<String>> e2:ad.attributes.entrySet()) {
							String attribute2 = e2.getKey();
							for (String value2:e2.getValue()) {
								AttributeValue av2 = new AttributeValue(attribute2,value2.toLowerCase());
								if (av.equals(av2)) continue;

								Integer linkCount = linkedAttrs.get(av2);
								if (linkCount==null) {
									linkedAttrs.put(av2, 1);
								} else {
									linkedAttrs.put(av2, linkCount+1);
								}
								totalLinks++;
							}
						}
					}
				}
			}
			nextID += AD_PROCESS_BATCH_SIZE;
			iterations++;
			if ((iterations%1000)==0) {
				long end = System.currentTimeMillis();
				System.out.println("Processed 1M ads in " + (end-start) + "ms. Ending on: " + nextID + ". Links: " + totalLinks);
				start = end;
			}
		}
		oculusdb.close();
		htdb.close();
		
		return result;
	}

	private static void computeLinks() {
		initTables();

		System.out.println("Reading attribute data...");
		HashMap<AttributeValue,HashMap<AttributeValue,Integer>> linkTable = getAttributeLinks();
		
		System.out.println("Writing attributes...");
		insertAttributes(linkTable);

		System.out.println("Writing attribute links...");
		insertAttributeLinks(linkTable);
	}

	private static String WHERE_IN_PSTMT_QUESTIONS = null;
	public static String commasAndQuestions(int count) {
		if (count==WHERE_IN_SELECT_SIZE) {
			if (WHERE_IN_PSTMT_QUESTIONS!=null) {
				return WHERE_IN_PSTMT_QUESTIONS;
			}
		}
		String result = StringUtil.commasAndQuestions(count);
		if (count==WHERE_IN_SELECT_SIZE) {
			WHERE_IN_PSTMT_QUESTIONS = result;
			return WHERE_IN_PSTMT_QUESTIONS;
		}
		return result;
	}

	/**
	 * Given a list of attribute values, fetch related attributes and their shared ad count. 
	 * Triples are (attribute1, attribute2, count).
	 */
	public static HashMap<AttributeValue,HashMap<AttributeValue,Integer>> getLinks(HashSet<AttributeValue> values) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		HashMap<AttributeValue,HashMap<AttributeValue,Integer>> result = new HashMap<AttributeValue,HashMap<AttributeValue,Integer>>();
		PreparedStatement stmt = null;
		try {
			String cq = commasAndQuestions(values.size());
			stmt = conn.prepareStatement("SELECT attr1,attr2,val1,val2,count from " 
					+ ATTRIBUTES_LINKS_TABLE + " WHERE val1 IN (" + cq + 
					") OR val2 IN (" + cq + ")");
			int i = 1;
			for (AttributeValue val:values) {
				stmt.setString(i, val.value);
				stmt.setString(values.size()+i, val.value);
				i++;
			}
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String val1 = rs.getString("val1").toLowerCase();
				String val2 = rs.getString("val2").toLowerCase();
				String attr1 = rs.getString("attr1").toLowerCase();
				String attr2 = rs.getString("attr2").toLowerCase();
				Integer count = rs.getInt("count");

				AttributeValue av1 = new AttributeValue(attr1, val1);
				AttributeValue av2 = new AttributeValue(attr2, val2);
				
				// Add the link (av1->av2)
				HashMap<AttributeValue,Integer> links = result.get(av1);
				if (links==null) {
					links = new HashMap<AttributeValue,Integer>();
					result.put(av1, links);
				}
				links.put(av2, count);

				// Add the reverse link (av2->av1)
				links = result.get(av2);
				if (links==null) {
					links = new HashMap<AttributeValue,Integer>();
					result.put(av2, links);
				}
				links.put(av1, count);
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

	public static HashSet<AttributeValue> getAttributes(Connection oculusconn, int startid, int endid) {
		HashSet<AttributeValue> result = new HashSet<AttributeValue>();
		String sqlStr = "SELECT attribute,value FROM " + ATTRIBUTES_TABLE + " where id>="+startid+" and id<=" + endid;
		Statement stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String attribute = rs.getString("attribute");
				String value = rs.getString("value");
				result.add(new AttributeValue(attribute,value.toLowerCase()));
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

	public static HashMap<Integer,AttributeValue> getAttributes(Connection oculusconn) {
		 HashMap<Integer,AttributeValue> result = new  HashMap<Integer,AttributeValue>();
		String sqlStr = "SELECT id,attribute,value FROM " + ATTRIBUTES_TABLE;
		Statement stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer id = rs.getInt("id");
				String attribute = rs.getString("attribute");
				String value = rs.getString("value").toLowerCase();
				result.put(id, new AttributeValue(attribute,value.toLowerCase()));
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


	public static void main(String[] args) {
		TimeLog tl = new TimeLog();
		tl.pushTime("Attribute links calculation");

		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		computeLinks();

		tl.popTime();
	}

}

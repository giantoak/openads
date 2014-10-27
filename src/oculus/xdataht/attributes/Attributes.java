package oculus.xdataht.attributes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.TimeLog;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

public class Attributes {
	
	private static final String ATTRIBUTES_TABLE = "attributes";
	private static final String ATTRIBUTES_ADS_TABLE = "attributes_ads";

	private static void createAttributesTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTES_TABLE+"` (" +
						  "id INT(11) NOT NULL," +
						  "attribute VARCHAR(32) NOT NULL," +
						  "value VARCHAR(128) NOT NULL," +
						  "PRIMARY KEY(id) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createAttributesAdsTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTES_ADS_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "ads_id INT(11) NOT NULL," +
						  "attributes_id INT(11) NOT NULL," +
						  "PRIMARY KEY (id) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(ATTRIBUTES_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTES_TABLE);
			db.clearTable(ATTRIBUTES_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTES_TABLE);
			createAttributesTable(db, conn);
		}
		if (db.tableExists(ATTRIBUTES_ADS_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTES_ADS_TABLE);
			db.clearTable(ATTRIBUTES_ADS_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTES_ADS_TABLE);
			createAttributesAdsTable(db, conn);
		}
		db.close();
		
	}

	/**
	 * Turn a comma separated string into a list of strings
	 */
	private static ArrayList<String> getStringList(String commaString) {
		ArrayList<String> result = new ArrayList<String>();
		if (commaString!=null) {
			commaString = commaString.trim().toLowerCase();
			commaString = Jsoup.clean(commaString, Whitelist.none());
			String[] values = commaString.split(",");
			for (String value:values) {
				if (value!=null) {
					value = value.trim();
					if (value.length()>3) {
						result.add(value);
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * Turn a comma separated string into a list of strings
	 */
	private static ArrayList<String> getWebList(String commaString) {
		ArrayList<String> result = new ArrayList<String>();
		if (commaString!=null) {
			commaString = commaString.trim().toLowerCase();
			commaString = Jsoup.clean(commaString, Whitelist.none());
			String[] values = commaString.split(",");
			for (String value:values) {
				if (value!=null) {
					value = value.trim();
					if (value.length()>3) {
						if (!(value.contains("backpage")||value.contains("myproviderguide")||value.contains("craigslist"))) {
							result.add(value);
						}
					}
				}
			}
		}
		return result;
	}
	
	
	
	
	public static HashMap<Integer,String> getAttributeNames() {
		HashMap<Integer,String> result = new HashMap<Integer,String>();
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT id,value FROM attributes";
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer id = rs.getInt("id");
				String value = rs.getString("value");
				result.put(id, value);
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
	
	public static HashSet<String> getAds(Integer attributes_id) {
		HashSet<String> result = new HashSet<String>();
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT ads_id FROM attributes_ads where attributes_id=" + attributes_id;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String ads_id = rs.getString("ads_id");
				result.add(ads_id);
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
	
	public static HashMap<String, Integer> getAdCounts(ArrayList<String> matchingAds) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();

		String adsStr = "(";
		boolean isFirst = true;
		for (String adid:matchingAds) {
			if (isFirst) isFirst = false;
			else adsStr += ",";
			adsStr += adid;
		}
		adsStr += ")";

		
		String sqlStr = "SELECT ads_id,attributes_id FROM attributes_ads where ads_id IN " + adsStr;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				// String ads_id = rs.getString("ads_id");
				String attributes_id = rs.getString("attributes_id");
				Integer count = result.get(attributes_id);
				if (count==null) count = 1;
				else count++;
				result.put(attributes_id, count);
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
	 * Get a mapping of attribute_name->attribute_value->Set of Ads
	 */
	public static HashMap<String,HashMap<String,HashSet<Integer>>> extractData(TimeLog tl) {
		TableDB db = TableDB.getInstance();
		HashMap<String,HashMap<String,HashSet<Integer>>> result = new HashMap<String,HashMap<String,HashSet<Integer>>>();
		Connection conn = db.open();
		String sqlStr = "SELECT id,phone_numbers,websites,email FROM ads";
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int adcount = 0;
			ResultSet rs = stmt.executeQuery(sqlStr);
			tl.pushTime("Processing result set start");
			while (rs.next()) {
				adcount++;
				Integer id = rs.getInt("id");
				String phone_numbers = rs.getString("phone_numbers");
				String emails = rs.getString("email");
				String websites = rs.getString("websites");
				ArrayList<String> phoneList = getStringList(phone_numbers);
				ArrayList<String> emailList = getStringList(emails);
				ArrayList<String> websiteList = getWebList(websites);
				incrementCounts(id, "phone", phoneList, result);
				incrementCounts(id, "email", emailList, result);
				incrementCounts(id, "website", websiteList, result);
				if (adcount%100000==0) {
					tl.popTime();
					tl.pushTime("Processing from " + adcount);
				}
			}
			tl.popTime();
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

	private static void incrementCounts(Integer adid, String attribute, ArrayList<String> valueList,
			HashMap<String, HashMap<String, HashSet<Integer>>> result) {
		HashMap<String, HashSet<Integer>> values = result.get(attribute);
		if (values==null) {
			values = new HashMap<String, HashSet<Integer>>();
			result.put(attribute, values);
		}
		for (String value:valueList) {
			if (value.length()>127) continue;
			HashSet<Integer> ads = values.get(value);
			if (ads==null) {
				ads = new HashSet<Integer>();
				values.put(value, ads);
			}
			ads.add(adid);
		}
	}

	public static void extractAttributes(TimeLog tl) {
		initTable();

		// Get attribute_name->attribute_value->set_of_adids
		HashMap<String, HashMap<String, HashSet<Integer>>> attributes = extractData(tl);
		System.out.println("Extracted attributes: " + attributes.get("phone").size() + "," + attributes.get("email").size() + "," + attributes.get("website").size());

		// Write the attributes table and get attribute ids
		HashMap<Integer, HashSet<Integer>> attributeToAds = writeAttributes(attributes);

		// Write the attribute_id->ad_id (many->many) mapping table
		writeAttributesAds(attributeToAds);
		
		// Create an ads_id->attributes map
		HashMap<Integer, HashSet<Integer>> adToAttributes = createAdToAttributesMap(attributeToAds);
		
		// Create an attributes_id->attributes_id->count map of attribute links
		HashMap<Integer,HashMap<Integer,Integer>> links = createLinksMap(attributeToAds, adToAttributes);
		
		// Write attribute links
		AttributeLinks.writeAttributeLinks(links);
	}

	public static HashMap<Integer,HashMap<Integer,Integer>> createLinksMap(
			HashMap<Integer, HashSet<Integer>> attributeToAds,
			HashMap<Integer, HashSet<Integer>> adToAttributes) {
		HashMap<Integer,HashMap<Integer,Integer>> links = new HashMap<Integer,HashMap<Integer,Integer>>();
		for (Entry<Integer,HashSet<Integer>> attre:attributeToAds.entrySet()) {
			Integer attrid = attre.getKey();
			for (Integer adid:attre.getValue()) {
				HashSet<Integer> otherAttributes = adToAttributes.get(adid);
				for (Integer otherAttribute:otherAttributes) {
					incrementLinkCount(links, attrid, otherAttribute);
				}
			}
		}
		return links;
	}

	private static void incrementLinkCount(HashMap<Integer,HashMap<Integer,Integer>> links, Integer attrid, Integer otherAttribute) {
		if (attrid==otherAttribute) return;
		Integer attr1, attr2;
		if (attrid<otherAttribute) { attr1 = attrid; attr2 = otherAttribute; }
		else { attr1 = otherAttribute; attr2 = attrid; }

		HashMap<Integer, Integer> attrLinks = links.get(attr1);
		if (attrLinks==null) {
			attrLinks = new HashMap<Integer, Integer>();
			links.put(attr1, attrLinks);
		}
		Integer count = attrLinks.get(attr2);
		if (count==null) count = 1;
		else count++;
		attrLinks.put(attr2, count);
	}
	
	public static HashMap<Integer, HashSet<Integer>> createAdToAttributesMap(
			HashMap<Integer, HashSet<Integer>> attributeToAds) {
		HashMap<Integer,HashSet<Integer>> adToAttributes = new HashMap<Integer,HashSet<Integer>>();
		for (Entry<Integer,HashSet<Integer>> attre:attributeToAds.entrySet()) {
			Integer attrid = attre.getKey();
			for (Integer adid:attre.getValue()) {
				HashSet<Integer> adAttributes = adToAttributes.get(adid);
				if (adAttributes==null) {
					adAttributes = new HashSet<Integer>();
					adToAttributes.put(adid, adAttributes);
				}
				adAttributes.add(attrid);
			}
		}
		return adToAttributes;
	}
	
	
	private static void writeAttributesAds(HashMap<Integer, HashSet<Integer>> attributeToAds) {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTES_ADS_TABLE + 
					"(attributes_id,ads_id) VALUES (?,?)");
			int count = 0;
			for (Entry<Integer,HashSet<Integer>> e:attributeToAds.entrySet()) {
				Integer attrid = e.getKey();
				HashSet<Integer> adids = e.getValue();
				for (Integer adid:adids) {
					pstmt.setInt(1, attrid);
					pstmt.setInt(2, adid);
					pstmt.addBatch();
					count++;
					if (count % TableDB.BATCH_INSERT_SIZE == 0) {
						pstmt.executeBatch();
					}
				}
			}
			pstmt.executeBatch();
			System.out.println("Inserted " + count + " ad attributes");
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

	private static HashMap<Integer, HashSet<Integer>> writeAttributes(
			HashMap<String, HashMap<String, HashSet<Integer>>> attributes) {
		HashMap<Integer, HashSet<Integer>> result = new HashMap<Integer, HashSet<Integer>>();
		int attrid = 0;
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTES_TABLE + 
					"(id,attribute,value) VALUES (?,?,?)");
			int count = 0;
			for (Entry<String,HashMap<String, HashSet<Integer>>> e:attributes.entrySet()) {
				String attribute = e.getKey();
				for (Entry<String, HashSet<Integer>> e2:e.getValue().entrySet()) {
					String value = e2.getKey();
					pstmt.setInt(1, attrid);
					pstmt.setString(2, attribute);
					pstmt.setString(3, value);
					pstmt.addBatch();
					count++;
					if (count % TableDB.BATCH_INSERT_SIZE == 0) {
						pstmt.executeBatch();
					}
					HashSet<Integer> ads = e2.getValue();
					result.put(attrid, ads);
					attrid++;
				}
			}
			System.out.println("Inserted " + count + " attributes");
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
		return result;
	}


	public static void main(String[] args) {
		TimeLog tl = new TimeLog();
		tl.pushTime("Attributes calculation");

		ScriptDBInit.initDB(args);
		extractAttributes(tl);

		tl.popTime();

	}

}

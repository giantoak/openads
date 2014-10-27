package oculus.memex.clustering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import oculus.memex.concepts.AdKeywords;
import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.extraction.AdExtraction;
import oculus.memex.geo.AdLocations;
import oculus.memex.geo.AdLocations.AdLocationSet;
import oculus.memex.graph.AttributeLinks;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.TimeLog;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

/**
 * Create a table of (attribute,value)->details (phone,email,location,source,name) distributions, etc.
 */
public class AttributeDetails {
	static final public String ATTRIBUTE_DETAILS_TABLE = "attributes_details";
	private static final int MAX_ATTRIBUTES_PER_BATCH = 5000;
	private static final int AD_SELECT_BATCH_SIZE = 1000;
	private static final String NO_AGGREGATE_VALUE = "none";
	public static final int BATCH_INSERT_SIZE = 2000;

	private static class ClusterData {
		int adcount = 0;
		HashMap<String,Integer> phonelist = new HashMap<String,Integer>();
		HashMap<String,Integer> emaillist = new HashMap<String,Integer>();
		HashMap<String,Integer> weblist = new HashMap<String,Integer>();
		HashMap<String,Integer> namelist = new HashMap<String,Integer>();
		HashMap<String,Integer> ethnicitylist = new HashMap<String,Integer>();
		HashMap<String,Integer> locationlist = new HashMap<String,Integer>();
		HashMap<String,Integer> sourcelist = new HashMap<String,Integer>();
		HashMap<String,HashMap<String,Integer>> keywordlist = new HashMap<String,HashMap<String,Integer>>();
		HashMap<Long,Integer> timeseries= new HashMap<Long,Integer>();
		Date latestAd = new Date(0);
	}
	
	private static void createTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+ATTRIBUTE_DETAILS_TABLE+"` (" +
						  "id INT(11) NOT NULL," +
						  "attribute VARCHAR(12) NOT NULL," +
						  "value VARCHAR(2500) NOT NULL," +
						  "adcount INT(11) NOT NULL," +
						  "phonelist TEXT DEFAULT NULL," +
						  "emaillist TEXT DEFAULT NULL," +
						  "weblist TEXT DEFAULT NULL," +
						  "namelist TEXT DEFAULT NULL," +
						  "ethnicitylist TEXT DEFAULT NULL," +
						  "locationlist TEXT DEFAULT NULL," +
						  "sourcelist TEXT DEFAULT NULL," +
						  "keywordlist TEXT DEFAULT NULL," +
						  "timeseries TEXT DEFAULT NULL," +
						  "latestad DATETIME," +
						  "PRIMARY KEY (id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(ATTRIBUTE_DETAILS_TABLE)) {
			System.out.println("Clearing table: " + ATTRIBUTE_DETAILS_TABLE);
			db.clearTable(ATTRIBUTE_DETAILS_TABLE);
		} else {			
			System.out.println("Creating table: " + ATTRIBUTE_DETAILS_TABLE);
			createTable(db, conn);
		}
		db.close();
		
	}
	
	public static void incrementCounts(String attrVal, HashMap<String,Integer> counts) {
		if (attrVal==null) {
			incrementCount(NO_AGGREGATE_VALUE, counts);
			return;
		}
		attrVal = attrVal.trim().toLowerCase();
		attrVal = Jsoup.clean(attrVal, Whitelist.none());
		String[] vals = attrVal.split(",");
		for (String val:vals) {
			incrementCount(val, counts);
		}
	}

	private static void incrementCounts(HashSet<Pair<String,String>> kwClassifiers, HashMap<String, HashMap<String, Integer>> counts) {
		if (kwClassifiers==null) {
			return;
		}
		for (Pair<String,String> kwClassifier:kwClassifiers) {
			String classifier = kwClassifier.getSecond();
			HashMap<String,Integer> cmap = counts.get(classifier);
			if (cmap==null) {
				cmap = new HashMap<String,Integer>();
				counts.put(classifier,  cmap);
			}
			incrementCount(kwClassifier.getFirst(), cmap);
		}
	}
	
	public static void incrementCount(String val, HashMap<String,Integer> counts) {
		if (val==null) val = NO_AGGREGATE_VALUE;
		val = val.trim();
		val = Jsoup.clean(val, Whitelist.none());
		Integer count = counts.get(val);
		if (count==null) {
			count = new Integer(1);
		} else {
			count++;
		}
		counts.put(val, count);
	}
	
	private static String mapToString(HashMap<String, Integer> map, Pair<String, Integer> maxValue) {
		if (map==null) return "";
		ArrayList<Pair<String,Integer>> a = new ArrayList<Pair<String,Integer>>();
		for (Map.Entry<String, Integer> e:map.entrySet()) {
			String key = e.getKey();
			key = key.trim();
			if (key.length()<2) continue;
			if (key.length()>200) key = key.substring(0, 200);
			a.add(new Pair<String,Integer>(key,e.getValue()));
		}
		Collections.sort(a, new Comparator<Pair<String,Integer>>() {
			public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
				return o2.getSecond()-o1.getSecond();
			}
		});

		String result = "";
		boolean isFirst = true;
		for (int i=0; i<20 && i<a.size(); i++) {
			Pair<String,Integer> p = a.get(i);
			if (isFirst) {
				isFirst = false;
				if ((maxValue!=null) && (p.getFirst().compareTo("none")!=0)) maxValue.set(p.getFirst(), p.getSecond());
			} else result += ",";
			result += "\"" + p.getFirst() + "\":" + p.getSecond();
		}
		return result.replaceAll("[^\\u0000-\\uFFFFFF]", "?");
	}

	private static String longMapToString(HashMap<Long, Integer> map) {
		String result = "";
		boolean isFirst = true;
		for (Map.Entry<Long,Integer> e:map.entrySet()) {
			if (isFirst) isFirst = false;
			else result += ",";
			result += "\"" + e.getKey() + "\":" + e.getValue();
		}
		return result;
	}
	
	private static String classifierMapToString(HashMap<String, HashMap<String, Integer>> classifierMap) {
		if (classifierMap==null) return "";
		HashMap<String,ArrayList<Pair<String,Integer>>> classifierArrays = new HashMap<String,ArrayList<Pair<String,Integer>>>();
		for (Map.Entry<String, HashMap<String, Integer>> e:classifierMap.entrySet()) {
			String classifier = e.getKey();
			HashMap<String,Integer> map = e.getValue();
			ArrayList<Pair<String,Integer>> a = new ArrayList<Pair<String,Integer>>();
			classifierArrays.put(classifier, a);
			for (Map.Entry<String,Integer> ke:map.entrySet()) {
				String key = ke.getKey();
				key = key.trim();
				if (key.length()<2) continue;
				if (key.length()>200) key = key.substring(0, 200);
				a.add(new Pair<String,Integer>(key,ke.getValue()));
			}
			Collections.sort(a, new Comparator<Pair<String,Integer>>() {
				public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
					return o2.getSecond()-o1.getSecond();
				}
			});
		}

		String result = "";
		boolean isFirstClassifier = true;
		for (Map.Entry<String, ArrayList<Pair<String, Integer>>> e:classifierArrays.entrySet()) {
			if (isFirstClassifier) {
				isFirstClassifier = false;
			} else result += ",";
			result += "\"" + e.getKey() + "\":{";
			ArrayList<Pair<String,Integer>> a = e.getValue();
			boolean isFirst = true;
			for (int i=0; i<20 && i<a.size(); i++) {
				Pair<String,Integer> p = a.get(i);
				if (isFirst) {
					isFirst = false;
				} else result += ",";
				result += "\"" + p.getFirst() + "\":" + p.getSecond();
			}
			result += "}";
		}
		return result;
	}

	/**
	 * Calculate ClusterData details for all attributes from startAttrId->endAttrId
	 * @param sources 
	 */
	public static HashMap<Integer, ClusterData> getAttributeAggregation(int startAttrId, int endAttrId, 
			HashMap<Integer, HashSet<Pair<String,String>>> adKeywords, 
			AdLocationSet adLocations, 
			HashMap<Integer, AttributeValue> allAttributes, 
			HashMap<Integer, String> sources) {

		HashMap<Integer,ClusterData> result = new HashMap<Integer,ClusterData>();
		
		// Open both databases
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();

		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();

		// Get the ad->attribute list mapping
		HashMap<Integer,HashSet<Integer>> adToAttributes = new HashMap<Integer,HashSet<Integer>>();
		ArrayList<Integer> ads = new ArrayList<Integer>();
		long start = System.currentTimeMillis();
		System.out.print(" Fetch ads: ");
		getAdsInAttributes(startAttrId, endAttrId, allAttributes, adToAttributes, oculusconn, htconn, ads);
		long end = System.currentTimeMillis();
		System.out.print((end-start)+"ms");
		start = end;

		long mainTime = 0;
		long extraTime = 0;
		long phoneTime = 0;
		
		// Fetch all the ad details and update the attribute data
		int i=0;
		while (i<ads.size()) {
			StringBuffer adstring = new StringBuffer("(");
			boolean isFirst = true;
			for (int j=0; j<AD_SELECT_BATCH_SIZE&&(i+j<ads.size()); j++) {
				if (isFirst) isFirst = false;
				else adstring.append(",");
				adstring.append(ads.get(i+j));
			}
			adstring.append(")");
			start = System.currentTimeMillis();
			getMainDetails(allAttributes, adKeywords, adLocations, result, adToAttributes, htconn, sources, adstring);
			end = System.currentTimeMillis();
			mainTime += (end-start);
			start = end;
			getExtraDetails(allAttributes, result, adToAttributes, htconn, adstring);
			end = System.currentTimeMillis();
			extraTime += (end-start);
			start = end;
			getAttributes(allAttributes, result, AdExtraction.ADS_PHONE_TABLE, adToAttributes, oculusconn, adstring);
			getAttributes(allAttributes, result, AdExtraction.ADS_EMAILS_TABLE, adToAttributes, oculusconn, adstring);
			getAttributes(allAttributes, result, AdExtraction.ADS_WEBSITES_TABLE, adToAttributes, oculusconn, adstring);
			end = System.currentTimeMillis();
			phoneTime += (end-start);
			i+=AD_SELECT_BATCH_SIZE;
		}
		System.out.print(" details: " + ads.size() + ":(" + mainTime + "," + extraTime + "," + phoneTime + ") ");
		
		oculusdb.close();
		htdb.close();
		return result;
	}

	/**
	 * Populate adToAttributes and ads with a mapping of ads_id to attributes_ids and a list of ads_id
	 */
	public static void getAdsInAttributes(int startid, int endid, HashMap<Integer,AttributeValue> allAttributes,
			HashMap<Integer, HashSet<Integer>> adToAttributes, 
			Connection oculusconn, Connection htconn, ArrayList<Integer> ads) {
		HashMap<String,Integer> phones = new HashMap<String,Integer>();
		HashMap<String,Integer> emailVals = new HashMap<String,Integer>();
		HashMap<String,Integer> webVals = new HashMap<String,Integer>();
		for (int i=startid; i<=endid; i++) {
			AttributeValue av = allAttributes.get(i);
			if (av==null) continue;
			if (av.attribute.equals("phone")) {
				phones.put(av.value, i);
			} else if (av.attribute.equals("email")){
				emailVals.put(av.value, i);
			} else {
				webVals.put(av.value, i);
			}
		}

		HashMap<String, HashSet<Integer>> adsForPhones = MemexAd.getAdsForValues(oculusconn, AdExtraction.ADS_PHONE_TABLE, phones.keySet());
		for (Entry<String,HashSet<Integer>> e:adsForPhones.entrySet()) {
			String phone = e.getKey();
			Integer attrid = phones.get(phone);
			for (int adid:e.getValue()) {
				HashSet<Integer> attrSet = adToAttributes.get(adid);
				if (attrSet==null) {
					attrSet = new HashSet<Integer>();
					adToAttributes.put(adid, attrSet);
				}
				attrSet.add(attrid);
			}
		}

		HashMap<String, HashSet<Integer>> adsForEmails = MemexAd.getAdsForValues(oculusconn, AdExtraction.ADS_EMAILS_TABLE, emailVals.keySet());
		for (Entry<String,HashSet<Integer>> e:adsForEmails.entrySet()) {
			String email = e.getKey();
			if (emailVals==null || email==null) {
				System.out.println("Missing emails: " + emailVals + "," + email);
				continue;
			}
			Integer attrid = emailVals.get(email);
			if (attrid==null) {
				continue;
			}
			for (int adid:e.getValue()) {
				HashSet<Integer> attrSet = adToAttributes.get(adid);
				if (attrSet==null) {
					attrSet = new HashSet<Integer>();
					adToAttributes.put(adid, attrSet);
				}
				attrSet.add(attrid);
			}
		}

		HashMap<String, HashSet<Integer>> adsForWebsites = MemexAd.getAdsForValues(oculusconn, AdExtraction.ADS_WEBSITES_TABLE, webVals.keySet());
		for (Entry<String,HashSet<Integer>> e:adsForWebsites.entrySet()) {
			String website = e.getKey();
			Integer attrid = webVals.get(website);
			for (int adid:e.getValue()) {
				HashSet<Integer> attrSet = adToAttributes.get(adid);
				if (attrSet==null) {
					attrSet = new HashSet<Integer>();
					adToAttributes.put(adid, attrSet);
				}
				attrSet.add(attrid);
			}
		}
		
		ads.addAll(adToAttributes.keySet());
	}

	private static void getExtraDetails(HashMap<Integer, AttributeValue> allAttributes, HashMap<Integer, ClusterData> result,
			HashMap<Integer, HashSet<Integer>> adToAttributes, Connection htconn,
			StringBuffer adstring) {
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT ads_id,attribute,value FROM ads_attributes where ads_id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				HashSet<Integer> attrids = adToAttributes.get(adid);
				if (attrids==null) continue;
				for (Integer attrid:attrids) {
					AttributeValue av = allAttributes.get(attrid);
					if (av==null) {
						continue;
					}
					ClusterData cd = result.get(attrid);
					if (cd==null) {
						cd = new ClusterData();
						result.put(attrid, cd);
						cd.adcount++;
					}
					String attribute = rs.getString("attribute");
					String value = rs.getString("value").toLowerCase();
//					if (attribute.compareTo("phone")==0) {
//						incrementCounts(value, cd.phonelist);
//					} else if (attribute.compareTo("email")==0) {
//						incrementCounts(value, cd.emaillist);
//					} else if (attribute.compareTo("website")==0) {
//						incrementCounts(value, cd.weblist);
//					} else 
					if (attribute.compareTo("ethnicity")==0) {
						incrementCounts(value, cd.ethnicitylist);
					} else if (attribute.compareTo("name")==0) {
						incrementCounts(value, cd.namelist);
					}
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

	private static void getAttributes(HashMap<Integer, AttributeValue> allAttributes, HashMap<Integer, ClusterData> result, String table,
			HashMap<Integer, HashSet<Integer>> adToAttributes, Connection oculusconn,
			StringBuffer adstring) {
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT ads_id,value FROM " + table + " where ads_id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				HashSet<Integer> attrids = adToAttributes.get(adid);
				if (attrids==null) continue;
				for (Integer attrid:attrids) {
					AttributeValue av = allAttributes.get(attrid);
					if (av==null) {
						continue;
					}
					ClusterData cd = result.get(attrid);
					if (cd==null) {
						cd = new ClusterData();
						result.put(attrid, cd);
						cd.adcount++;
					}
					String value = rs.getString("value");
					if (table.compareTo(AdExtraction.ADS_PHONE_TABLE)==0) incrementCounts(value, cd.phonelist);
					else if (table.compareTo(AdExtraction.ADS_WEBSITES_TABLE)==0) incrementCounts(value, cd.weblist);
					if (table.compareTo(AdExtraction.ADS_EMAILS_TABLE)==0) incrementCounts(value, cd.emaillist);
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

	/**
	 * Fetch details from memex_ht.ads for every ad in adstring. Store the resulting details with each attribute.
	 */
	private static void getMainDetails(
			HashMap<Integer, AttributeValue> allAttributes, HashMap<Integer, HashSet<Pair<String, String>>> adKeywords,
			AdLocationSet adLocations, HashMap<Integer, ClusterData> result,
			HashMap<Integer, HashSet<Integer>> adToAttributes, Connection htconn,
			HashMap<Integer,String> sources,
			StringBuffer adstring) {
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT id,sources_id,posttime FROM ads WHERE id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			Calendar c = Calendar.getInstance();
			while (rs.next()) {
				int adid = rs.getInt("id");

				// For all of the attributes that contain this add, increment the cluster details
				HashSet<Integer> attrids = adToAttributes.get(adid);
				if (attrids==null) continue;
				for (Integer attrid:attrids) {
					AttributeValue av = allAttributes.get(attrid);
					if (av==null) {
						continue;
					}
					ClusterData cd = result.get(attrid);
					if (cd==null) {
						cd = new ClusterData();
						result.put(attrid, cd);
					}
					incrementCounts(sources.get(rs.getInt("sources_id")), cd.sourcelist);
					incrementCounts(adKeywords.get(adid), cd.keywordlist);
					incrementCount(adLocations.getLocation(adid), cd.locationlist);
					cd.adcount++;
					Date date = rs.getDate("posttime");
					long time = 0;
					if(date!=null) {
						if(cd.latestAd.compareTo(date)<0) {
							cd.latestAd=date;
						}						
						c.setTime(date);
						c.set(Calendar.HOUR,0);
						c.set(Calendar.MINUTE,0);
						c.set(Calendar.SECOND,0);
						c.set(Calendar.MILLISECOND,0);
						time = c.getTimeInMillis()/1000;
					}
					Integer i = cd.timeseries.get(time);
					if (i==null) {
						i = new Integer(1);
					} else {
						i++;
					}
					cd.timeseries.put(time, i);
				}					
			}
			
		} catch (Exception e) {
			System.out.println("Failed: " + sqlStr);
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static HashMap<Integer,String> getSources(Connection htconn) {
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT id,name from sources";
		stmt = null;
		HashMap<Integer,String> result = new HashMap<Integer,String>();
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int id = rs.getInt("id");
				String name = rs.getString("name");
				result.put(id, name);
			}
		} catch (Exception e) {
			System.out.println("Failed: " + sqlStr);
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
	

	public static HashSet<String> getPreclusterIDs() {
		HashSet<String> result = new HashSet<String>();
		MemexOculusDB db = MemexOculusDB.getInstance();
		String sqlStr = "SELECT distinct clusterid FROM " + Cluster.CLUSTER_TABLE;
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String org = rs.getString("clusterid");
				result.add(org);
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

	public static int getMaxAttributeID() {
		String sqlStr = "SELECT max(id) as max FROM " + AttributeLinks.ATTRIBUTES_TABLE;
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		int result = MemexOculusDB.getInt(conn, sqlStr, "Get max attribute id");
		db.close();
		return result;
	}

	
	public static void insertAttributeData(HashMap<Integer, AttributeValue> allAttributes, HashMap<Integer, ClusterData> clusterTable) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		AttributeValue av = null;
		ClusterData cd = null;
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + ATTRIBUTE_DETAILS_TABLE + 
					"(id, attribute, value, adcount, phonelist, emaillist, weblist, namelist, ethnicitylist, locationlist, sourcelist, keywordlist, timeseries, latestad) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int count = 0;
			for (Entry<Integer,ClusterData> e:clusterTable.entrySet()) {
				Pair<String,Integer> maxPhone = new Pair<String,Integer>(null,0);
				Pair<String,Integer> maxEmail = new Pair<String,Integer>(null,0);
				Pair<String,Integer> maxName =new Pair<String,Integer>(null,0);
				Integer attrid = e.getKey();
				av = allAttributes.get(attrid);
				cd = e.getValue();
				pstmt.setInt(1, attrid);
				pstmt.setString(2, av.attribute);
				pstmt.setString(3, av.value);
				pstmt.setInt(4, cd.adcount);
				pstmt.setString(5, mapToString(cd.phonelist, maxPhone));
				pstmt.setString(6, mapToString(cd.emaillist, maxEmail));
				pstmt.setString(7, mapToString(cd.weblist, null));
				pstmt.setString(8, mapToString(cd.namelist, maxName));
				pstmt.setString(9, mapToString(cd.ethnicitylist, null));
				pstmt.setString(10, mapToString(cd.locationlist, null));
				pstmt.setString(11, mapToString(cd.sourcelist, null));
				pstmt.setString(12, classifierMapToString(cd.keywordlist));
				pstmt.setString(13, longMapToString(cd.timeseries));
				pstmt.setDate(14, new java.sql.Date(cd.latestAd.getTime()));
				pstmt.addBatch();
				count++;
				if (count % BATCH_INSERT_SIZE == 0) {
					pstmt.executeBatch();
				}
			}
			pstmt.executeBatch();
		} catch (Exception e) {
			System.out.println("Failed to write cluster details batch");
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

	public static void lowMemDetails() {
		initTable();

		long start = System.currentTimeMillis();
		System.out.print("Fetch ad keywords...");
		HashMap<Integer, HashSet<Pair<String,String>>> adKeywords = AdKeywords.getAdKeywords(); 
		long end = System.currentTimeMillis();
		System.out.println((end-start) + "ms");
		start = end;

		System.out.print("Fetch ad locations...");
		AdLocationSet adLocations = AdLocations.getAdLocations();
		end = System.currentTimeMillis();
		System.out.println((end-start) + "ms");
		start = end;

		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		System.out.print("Fetch all attributes...");
		HashMap<Integer,AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
		oculusdb.close();
		end = System.currentTimeMillis();
		System.out.println((end-start) + "ms");
		start = end;

		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		HashMap<Integer,String> sources = getSources(htconn);
		htdb.close();
		
		int maxid = getMaxAttributeID();
		int count = 0;
		// Loop over all attributes, calculate details and write to database
		while (count<maxid) {
			// Calculate details for count->count+MAX_ATTRIBUTES_PER_BATCH-1
			System.out.print("\tProcessing: " + count + " to " + (count+MAX_ATTRIBUTES_PER_BATCH-1));
			HashMap<Integer,ClusterData> clusterTable = getAttributeAggregation(count, count+MAX_ATTRIBUTES_PER_BATCH-1, adKeywords, adLocations, allAttributes, sources);
			end = System.currentTimeMillis();
			System.out.println(" in " + ((end-start)/1000) + " seconds.");
			start = end;

			insertAttributeData(allAttributes, clusterTable);
			count+=MAX_ATTRIBUTES_PER_BATCH;
			end = System.currentTimeMillis();
			System.out.println("\tWrote in " + ((end-start)/1000) + " seconds.");
			start = end;
		}
	}
	

//	public static void test(int id) {
//		HashMap<Integer, HashSet<Pair<String,String>>> adKeywords = new HashMap<Integer, HashSet<Pair<String,String>>>();
//
//		AdLocationSet adLocations = new AdLocationSet();
//
//		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
//		Connection oculusconn = oculusdb.open();
//		HashMap<Integer,AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
//		oculusdb.close();
//
//		HashMap<Integer,String> sources = new HashMap<Integer,String>();
//		
//			// Calculate details for count->count+MAX_ATTRIBUTES_PER_BATCH-1
//		System.out.print("\tProcessing: " + id);
//		HashMap<Integer,ClusterData> clusterTable = getAttributeAggregation(id-1, id+1, adKeywords, adLocations, allAttributes, sources);
//
//	}

	public static void main(String[] args) {
		TimeLog tl = new TimeLog();
		tl.pushTime("Attribute details calculation");

		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		lowMemDetails();

		tl.popTime();
	}

}

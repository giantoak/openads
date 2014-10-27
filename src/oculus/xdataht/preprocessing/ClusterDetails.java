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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.Pair;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

// TODO: Create a table of location -> organization clusterid
public class ClusterDetails {
	private static final int MAX_CLUSTERS_PER_BATCH = 50000;
	private static final String NO_AGGREGATE_VALUE = "none";

	private static class ClusterData {
		int adcount = 0;
		ArrayList<String> ads = new ArrayList<String>();
		HashMap<String,Integer> phonelist = new HashMap<String,Integer>();
		HashMap<String,Integer> emaillist = new HashMap<String,Integer>();
		HashMap<String,Integer> weblist = new HashMap<String,Integer>();
		HashMap<String,Integer> namelist = new HashMap<String,Integer>();
		HashMap<String,Integer> ethnicitylist = new HashMap<String,Integer>();
		HashMap<String,Integer> locationlist = new HashMap<String,Integer>();
		HashMap<String,Integer> sourcelist = new HashMap<String,Integer>();
		HashMap<String,HashMap<String,Integer>> keywordlist = new HashMap<String,HashMap<String,Integer>>();
		HashMap<Long,Integer> timeseries= new HashMap<Long,Integer>();
		HashSet<String> adidlist = new HashSet<String>();
	}
	
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.CLUSTER_DETAILS_TABLE+"` (" +
						  "clusterid INT(11) NOT NULL," +
						  "clustername TEXT NOT NULL," +
						  "adcount INT(11) NOT NULL," +
						  "phonelist TEXT," +
						  "emaillist TEXT," +
						  "weblist TEXT," +
						  "namelist TEXT," +
						  "ethnicitylist TEXT," +
						  "locationlist TEXT," +
						  "sourcelist TEXT," +
						  "keywordlist TEXT," +
						  "adidlist TEXT," +
						  "timeseries TEXT," +
						  "PRIMARY KEY (clusterid) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.CLUSTER_DETAILS_TABLE)) {
			System.out.println("Clearing table: " + TableDB.CLUSTER_DETAILS_TABLE);
			db.clearTable(TableDB.CLUSTER_DETAILS_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.CLUSTER_DETAILS_TABLE);
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

	public static void incrementWebCounts(String attrVal, HashMap<String,Integer> counts) {
		if (attrVal==null) {
			incrementCount(NO_AGGREGATE_VALUE, counts);
			return;
		}
		attrVal = attrVal.trim().toLowerCase();
		attrVal = Jsoup.clean(attrVal, Whitelist.none());
		String[] vals = attrVal.split(",");
		for (String val:vals) {
			if (val.contains("backpage.com") || val.contains("myproviderguide.com") || val.contains("craigslist")) continue;
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
		return result;
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
	
	private static String setToString(HashSet<String> set) {
		String result = "";
		boolean isFirst = true;
		for (String val:set) {
			if (isFirst) isFirst = false;
			else result += ",";
			result += "\"" + val + "\"";
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


	public static HashMap<String,ClusterData> getPreclusterAggregation(TableDB db, int startclusterid, int endclusterid, HashMap<String, HashSet<Pair<String,String>>> adKeywords) {
		HashMap<String,ClusterData> result = new HashMap<String,ClusterData>();
		String sqlStr = "SELECT ads.id,phone_numbers,email,websites,name,ethnicity,location,source,post_timestamp,adid,precluster.org_cluster_id as org "
				+ "FROM ads INNER JOIN precluster ON ads.id=precluster.ad_id where precluster.org_cluster_id>="+startclusterid+" and precluster.org_cluster_id<" + endclusterid;
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String org = rs.getString("org");
				ClusterData cd = result.get(org);
				if (cd==null) {
					cd = new ClusterData();
					result.put(org, cd);
				}
				String adid = rs.getString("id");
				cd.ads.add(adid);
				incrementCounts(rs.getString("phone_numbers"), cd.phonelist);
				incrementCounts(rs.getString("email"), cd.emaillist);
				incrementWebCounts(rs.getString("websites"), cd.weblist);
				incrementCount(rs.getString("name"), cd.namelist);
				incrementCounts(rs.getString("ethnicity"), cd.ethnicitylist);
				incrementCount(rs.getString("location"), cd.locationlist);
				incrementCount(rs.getString("source"), cd.sourcelist);
				incrementCounts(adKeywords.get(adid), cd.keywordlist);
				cd.adidlist.add(rs.getString("adid"));
				cd.adcount++;
				long time = rs.getLong("post_timestamp");
				time = (time/(60*60*24))*(60*60*24);
				Integer i = cd.timeseries.get(time);
				if (i==null) {
					i = new Integer(1);
				} else {
					i++;
				}
				cd.timeseries.put(time, i);
				
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

	public static HashSet<String> getPreclusterIDs(TableDB db) {
		HashSet<String> result = new HashSet<String>();
		String sqlStr = "SELECT distinct org_cluster_id FROM precluster";
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String org = rs.getString("org_cluster_id");
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

	public static int getMaxID(TableDB db) {
		String sqlStr = "SELECT max(org_cluster_id) as max FROM precluster";
		int result = 0;
		Connection conn = db.open();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			if (rs.next()) {
				result = rs.getInt("max");
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

	
	public static void insertClusterData(TableDB db, HashMap<String,ClusterData> resultMap) {
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TableDB.CLUSTER_DETAILS_TABLE + 
					"(clusterid, adcount, phonelist, emaillist, weblist, namelist, ethnicitylist, locationlist, sourcelist, keywordlist, timeseries, adidlist, clustername) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int count = 0;
			for (Entry<String,ClusterData> e:resultMap.entrySet()) {
				Pair<String,Integer> maxPhone = new Pair<String,Integer>(null,0);
				Pair<String,Integer> maxEmail = new Pair<String,Integer>(null,0);
				Pair<String,Integer> maxName =new Pair<String,Integer>(null,0);
				String clusterId = e.getKey();
				ClusterData cd = e.getValue();
				pstmt.setString(1, clusterId);
				pstmt.setInt(2, cd.adcount);
				pstmt.setString(3, mapToString(cd.phonelist, maxPhone));
				pstmt.setString(4, mapToString(cd.emaillist, maxEmail));
				pstmt.setString(5, mapToString(cd.weblist, null));
				pstmt.setString(6, mapToString(cd.namelist, maxName));
				pstmt.setString(7, mapToString(cd.ethnicitylist, null));
				pstmt.setString(8, mapToString(cd.locationlist, null));
				pstmt.setString(9, mapToString(cd.sourcelist, null));
				pstmt.setString(10, classifierMapToString(cd.keywordlist));
				pstmt.setString(11, longMapToString(cd.timeseries));
				pstmt.setString(12, setToString(cd.adidlist));
				pstmt.setString(13, getClusterName(clusterId,maxEmail,maxPhone,maxName));
				pstmt.addBatch();
				count++;
				if (count % TableDB.BATCH_INSERT_SIZE == 0) {
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

	private static String getClusterName(String clusterid, Pair<String, Integer> maxEmail, Pair<String, Integer> maxPhone, Pair<String, Integer> maxName) {
		String result = maxPhone.getFirst();
		int count = maxPhone.getSecond();
		if (maxEmail.getSecond()>2||maxEmail.getSecond()>count) {
			result = maxEmail.getFirst();
		} else if (maxName.getSecond()>2||maxName.getSecond()>count) {
			result = maxName.getFirst();
		}
		if (result==null) result = clusterid;
		return result;
	}

	public static void lowMemDetails() {
		initTable();
		TableDB db = TableDB.getInstance();
		HashMap<String, HashSet<Pair<String,String>>> adKeywords = AdKeywords.getAdKeywords(db);
		int maxid = getMaxID(db);
		int count = 0;
		long start = System.currentTimeMillis();
		while (count<maxid) {
			System.out.print("\tProcessing: " + count + " to " + (count+MAX_CLUSTERS_PER_BATCH));
			HashMap<String,ClusterData> clusterTable = getPreclusterAggregation(db, count, count+MAX_CLUSTERS_PER_BATCH, adKeywords);
			long end = System.currentTimeMillis();
			System.out.println(" in " + ((end-start)/1000) + " seconds.");
			start = end;
			count+=MAX_CLUSTERS_PER_BATCH;
			insertClusterData(db, clusterTable);
			end = System.currentTimeMillis();
			System.out.println("\tWrote in " + ((end-start)/1000) + " seconds.");
			start = end;
		}
	}
	
	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin cluster details calculation...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		lowMemDetails();

		long end = System.currentTimeMillis();
		System.out.println("Done cluster details calculation in: " + (end-start) + "ms");

	}

}

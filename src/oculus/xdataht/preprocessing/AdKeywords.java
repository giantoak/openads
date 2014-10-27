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
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.Pair;

// TODO: Create a table of adid -> clusterid
public class AdKeywords {
	
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.AD_KEYWORD_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "adid INT(11) NOT NULL," +
						  "keyword VARCHAR(45) NOT NULL," +
						  "classifier VARCHAR(45) NOT NULL," +
						  "count INT(11) NOT NULL," +
						  "PRIMARY KEY (id) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.AD_KEYWORD_TABLE)) {
			System.out.println("Clearing table: " + TableDB.AD_KEYWORD_TABLE);
			db.clearTable(TableDB.AD_KEYWORD_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.AD_KEYWORD_TABLE);
			createTable(db, conn);
		}
		db.close();
	}
	
	
	static class KeywordPattern {
		String classifier;
		String keyword;
		Pattern pattern;
		public KeywordPattern(String keyword, String classifier) {
			this.keyword = keyword;
			this.classifier = classifier;
			this.pattern = Pattern.compile(keyword);
		}
	}
	
	public static HashMap<String,HashMap<String,Pair<String,Integer>>> getKeywordCounts(TableDB db) {
		HashMap<String, HashSet<String>> keywords = Keywords.getKeywords(db);
		Connection conn = db.open();
		HashMap<String,HashMap<String,Pair<String,Integer>>> result = new HashMap<String,HashMap<String,Pair<String,Integer>>>();
		
		ArrayList<KeywordPattern> patterns = new ArrayList<KeywordPattern>();
		for (String classifier:keywords.keySet()) {
			HashSet<String> kws = keywords.get(classifier);
			for (String kw:kws) {
				patterns.add(new KeywordPattern(kw, classifier));
			}
		}
		String sqlStr = "SELECT id,body FROM ads";
		long start = System.currentTimeMillis();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int adcount = 0;
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				adcount++;
				String id = rs.getString("id");
				String body = rs.getString("body");
				extractKeywords(id, body, patterns, result);
				if (adcount%100000==0) {
					long end = System.currentTimeMillis();
					System.out.println("Processed " + adcount + " in " + (end-start) + "ms");
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
		db.close();
		return result;
	}
	
	private static void extractKeywords(String id, String body,
			ArrayList<KeywordPattern> patterns,
			HashMap<String, HashMap<String, Pair<String, Integer>>> result) {
		for (int i=0; i<patterns.size(); i++) {
			if (body==null || body.length()<2) continue;
			KeywordPattern p = patterns.get(i);
			Matcher  matcher = p.pattern.matcher(body);
			int count = 0;
			while (matcher.find()) {
				count++;
			}
			if (count>0) {
				HashMap<String, Pair<String, Integer>> adKeywords = result.get(id);
				if (adKeywords==null) {
					adKeywords = new HashMap<String, Pair<String, Integer>>();
					result.put(id, adKeywords);
				}
				Pair<String,Integer> matches = adKeywords.get(p.keyword);
				if (matches==null) matches = new Pair<String,Integer>(p.classifier,0);
				matches.setSecond(matches.getSecond()+1);
				adKeywords.put(p.keyword,matches);
			}
		}
	}

	public static void insertAdKeywords(TableDB db, HashMap<String, HashMap<String, Pair<String,Integer>>> resultMap) {
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TableDB.AD_KEYWORD_TABLE + 
					"(adid, keyword, count, classifier) VALUES (?,?,?,?)");
			int count = 0;
			for (Entry<String,HashMap<String, Pair<String,Integer>>> e:resultMap.entrySet()) {
				String adId = e.getKey();
				HashMap<String, Pair<String,Integer>> keywords = e.getValue();
				for (Entry<String,Pair<String,Integer>> kw:keywords.entrySet()) {
					pstmt.setString(1, adId);
					pstmt.setString(2, kw.getKey());
					pstmt.setInt(3, kw.getValue().getSecond());
					pstmt.setString(4, kw.getValue().getFirst());
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

	private static void computeLocations() {
		System.out.println("Calculating keywords...");
		TableDB db = TableDB.getInstance();
		HashMap<String,HashMap<String, Pair<String,Integer>>> keywordData = getKeywordCounts(db);
		
		initTable();
		System.out.println("Writing keyword results...");
		insertAdKeywords(db, keywordData);
	}

	public static HashMap<String,HashSet<Pair<String,String>>> getAdKeywords(TableDB db) {
		Connection conn = db.open();
		HashMap<String,HashSet<Pair<String,String>>> result = new HashMap<String,HashSet<Pair<String,String>>>();
		String sqlStr = "SELECT adid,keyword,classifier FROM adkeywords";
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String adid = rs.getString("adid");
				String keyword = rs.getString("keyword");
				String classifier = rs.getString("classifier");
				HashSet<Pair<String,String>> set = result.get(adid);
				if (set==null) {
					set = new HashSet<Pair<String,String>>();
					result.put(adid, set);
				}
				set.add(new Pair<String,String>(keyword,classifier));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
		}
		db.close();
		return result;
	}		
	
	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin keyword calculation...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		computeLocations();

		long end = System.currentTimeMillis();
		System.out.println("Done keyword calculation in: " + (end-start) + "ms");

	}

}

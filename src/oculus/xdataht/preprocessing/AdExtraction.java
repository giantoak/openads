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
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.StringUtil;

/**
 * Scans the ads table 'body' column using regular expressions to detect measurements and post ids. 
 * Writes the results to adextracted and uses the values to cull invalid phone numbers.
 */
public class AdExtraction {
	
	/**
	 *  A class to hold a regular expression and labels defining the meaning of the groups
	 *  matched by the expression.
	 */
	private static class RegexExtractor {
		String regex;
		String[] values;
		public RegexExtractor(String regex, String[] values) {
			super();
			this.regex = regex;
			this.values = values;
		}
	}

	private static final int PHONE_FIX_BATCH_SIZE = 50; // Batch sql prepared statement size

	// Some common RE substrings (unused)
	public static String WEIGHT_RE = "(1[0-5][0-9])";
	public static String WEIGHT_RE2 = "(\\d{2,3})\\s*lbs";
	public static String HEIGHT_RE = "([4-5]'*[0-9]\"?)";
	public static String AGE_RE = "([1-3][0-9])";
	public static String SEPERATOR_RE = "[, &-]+";

	// Extractors to be run on each roxy_ui.ads.body column.
	private static RegexExtractor[] EXTRACTORS = {
		new RegexExtractor("([1-3][0-9])[, &-]+([4-5]'*[0-9]\"?)[, &-]+(1[0-5][0-9])",  new String[] {"age", "height", "weight"}),
		new RegexExtractor("([4-5]['\\?]*[0-9]\"?)[\\., &-]+(\\d{2,3})\\s*lbs?", new String[] {"height", "weight"}),
		new RegexExtractor("([1-3][0-9]) y/o", new String[] {"age"}),
		new RegexExtractor("([1-3][0-9]) yrs", new String[] {"age"}),
		new RegexExtractor("([1-3][0-9]) years", new String[] {"age"}),
		new RegexExtractor("age[- ]*([1-3][0-9])", new String[] {"age"}),
		new RegexExtractor("(\\d{2,3})\\s*lbs?", new String[] {"weight"}),
		new RegexExtractor("([4-5]'*[0-9]\"?)[, &-]+([2-4][0-9][A-Ea-e]{0,3})[, -]+([2-4][0-9])[, -]+([2-4][0-9])", new String[] {"height", "chest", "waist", "hips"}),
		new RegexExtractor("([1-3][0-9])[, &-]+([2-4][0-9][A-Ea-e]{0,3})[, &-]+([4-5]'*[0-9]\"?)[, &-]+(1[0-5][0-9])",  new String[] {"age", "chest", "height", "weight"}),
		new RegexExtractor("([2-4][0-9][A-Ea-e]{0,3})[, -]+([2-4][0-9])[, -]+([2-4][0-9])[\\., &-]+(\\d{2,3})\\s*HH?", new String[] {"chest", "waist", "hips", "hourprice"}),
		new RegexExtractor("([2-4][0-9][A-Ea-e]{0,3})[, -]+([2-4][0-9])[, -]+([2-4][0-9])[\\., &-]+(\\d{2,3})\\s*lbs?", new String[] {"chest", "waist", "hips", "weight"}),
		new RegexExtractor("([4-5]'*[0-9]\"?)[, &-]+(1[0-5][0-9])[, &-]+([2-4][0-9][A-Ea-e]{0,3})",  new String[] {"height", "weight", "chest"}),
		new RegexExtractor("<strong>Post ID:</strong> (\\d{7})", new String[] {"postid"}),
		new RegexExtractor("Post - (\\d{7})", new String[] {"postid"}),
		new RegexExtractor("featured_title_(\\d{7})", new String[] {"featureid"}),
//		new RegexExtractor("((\\(?\\d{3}\\)?)+[- ]*\\d{3}[- ]*\\d{4})",  new String[] {"phone", "area code"}),
	};
	
	
	/**
	 * Create a table to store the measurements, etc.
	 */
	private static void createTable(TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+TableDB.AD_EXTRACTION_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "adid INT(11) NOT NULL," +
						  "age VARCHAR(8)," +
						  "height VARCHAR(8)," +
						  "weight VARCHAR(8)," +
						  "chest VARCHAR(8)," +
						  "hips VARCHAR(8)," +
						  "waist VARCHAR(8)," +
						  "phone VARCHAR(16)," +
						  "PRIMARY KEY (id) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Check to see if the table exists and clear or create.
	 */
	public static void initTable() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		if (db.tableExists(TableDB.AD_EXTRACTION_TABLE)) {
			System.out.println("Clearing table: " + TableDB.AD_EXTRACTION_TABLE);
			db.clearTable(TableDB.AD_EXTRACTION_TABLE);
		} else {			
			System.out.println("Creating table: " + TableDB.AD_EXTRACTION_TABLE);
			createTable(db, conn);
		}
		db.close();
	}
	
	/**
	 * Loop over the ads table body and phone_numbers to identify and fix bad phone numbers.
	 */
	public static HashMap<String,HashMap<String,String>> extractData(TableDB db) {
		Connection conn = db.open();
		HashMap<String,HashMap<String,String>> result = new HashMap<String,HashMap<String,String>>();
		HashMap<String,String> updates = new HashMap<String,String>();
		Pattern patterns[] = new Pattern[EXTRACTORS.length];
		for (int i=0; i<EXTRACTORS.length; i++) {
			patterns[i] = Pattern.compile(EXTRACTORS[i].regex);
		}
		String sqlStr = "SELECT id,body,phone_numbers FROM ads";
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
				String phone_numbers = rs.getString("phone_numbers");
				ArrayList<String> phoneList = getPhoneList(phone_numbers);
				int numPhones = phoneList.size();

				// Extract values from body
				if (body!=null) {
					body = body.replaceAll("&quot;", "\"");
					body = body.replaceAll("&amp;", "\"");
					HashMap<String, String> extracted = extractValues(patterns, body, id, phoneList);
					result.put(id, extracted);
				}
				
				// Check for phone numbers that match measurement text
				if (phoneList.size()!=numPhones) {
						String newValStr = "";
						boolean isFirst = true;
						for (String goodVal:phoneList) {
							if (isFirst) isFirst = false;
							else newValStr += ",";
							newValStr += goodVal;
						}
						updates.put(id, newValStr);
				}
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

		// Write the updated phone numbers
		writePhoneFixes(conn, updates);

		db.close();
		return result;
	}

	/**
	 * Update roxy_ui.ads (id,phone_numbers) columns using a prepared statement and batches.
	 */
	public static void writePhoneFixes(Connection conn,	HashMap<String, String> updates) {
		System.out.println("Writing Phone Fixes: " + updates.size());
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("UPDATE " + ScriptDBInit._datasetName + " SET phone_numbers=? " +
					" where id=?");
			int count = 0;
			for (Entry<String,String> entry:updates.entrySet()) {
				pstmt.setString(1,entry.getValue());
				pstmt.setString(2,entry.getKey());
				pstmt.addBatch();
				count++;
				if ((count%PHONE_FIX_BATCH_SIZE)==0) {
					System.out.println("Writing phone fixes " + count + "-" + (count+PHONE_FIX_BATCH_SIZE));
					pstmt.executeBatch();
				}
			}
			System.out.println("Writing phone fixes " + count + "-" + updates.size());
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
		System.out.println("Done writing Phone Fixes");
	}

	/**
	 * Update roxy_ui.ads (id,phone_numbers) columns using a single statements.
	 */
	public static void writePhoneFixes2(TableDB db, Connection conn, HashMap<String, String> updates) {
		System.out.println("Writing Phone Fixes: " + updates.size());
		int count = 0;
		long time = System.currentTimeMillis();
		for (Entry<String,String> entry:updates.entrySet()) {
			writePhoneFix(db, conn, entry.getKey(), entry.getValue());
			long t2 = System.currentTimeMillis();
			System.out.println((count++) + " in " + (t2-time) + "ms");
			time = t2;
		}
		System.out.println("Done writing Phone Fixes");
	}	
	
	public static void writePhoneFix(TableDB db, Connection conn, String id, String phones) {
			String sqlStr = "UPDATE " + ScriptDBInit._datasetName + " SET phone_numbers='" + phones +
					"' where id='" + id + "';";
			System.out.println(sqlStr);
			db.tryStatement(conn, sqlStr);
	}

	/**
	 * Turn a comma separated string into a list of phone numbers
	 */
	private static ArrayList<String> getPhoneList(String phone_numbers) {
		ArrayList<String> result = new ArrayList<String>();
		if (phone_numbers!=null) {
			String[] phones = phone_numbers.split(",");
			for (String phone:phones) {
				if (phone!=null) {
					phone = phone.trim();
					if (phone.length()>3) {
						result.add(phone);
					}
				}
			}
		}
		return result;
	}

	
	/**
	 * Write table adextracted
	 */
	public static void insertExtracted(TableDB db, HashMap<String, HashMap<String, String>> resultMap) {
		PreparedStatement pstmt = null;
		Connection conn = db.open();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + TableDB.AD_EXTRACTION_TABLE + 
					"(adid,age,height,weight,chest,hips,waist,phone) VALUES (?,?,?,?,?,?,?,?)");
			int count = 0;
			for (Entry<String,HashMap<String, String>> e:resultMap.entrySet()) {
				String adId = e.getKey();
				HashMap<String, String> extracted = e.getValue();
				if (extracted.size()>0) {
					pstmt.setString(1, adId);
					pstmt.setString(2, extracted.get("age"));
					pstmt.setString(3, extracted.get("height"));
					pstmt.setString(4, extracted.get("weight"));
					pstmt.setString(5, extracted.get("chest"));
					pstmt.setString(6, extracted.get("hips"));
					pstmt.setString(7, extracted.get("waist"));
					pstmt.setString(8, extracted.get("phone"));
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

	/**
	 * Using the given RE patterns, find matches in the body text and remove bad phone numbers from the array.
	 */
	public static HashMap<String, String> extractValues(Pattern[] patterns, String body, String id, ArrayList<String> phones) {
		HashMap<String,String> extracted = new HashMap<String,String>();
		if (body!=null) {
			for (int i=0; i<patterns.length; i++) {
				Matcher  matcher = patterns[i].matcher(body);
				String[] values = EXTRACTORS[i].values;
				while (matcher.find()) {
					String fullMatch = matcher.group(0);
					String fullStripped = StringUtil.stripNonNumeric(fullMatch);
//					System.out.println(i + ":" + fullMatch);
					checkBadPhones(fullMatch, fullStripped, phones, i, id);
					for (int j=0; j<values.length; j++) {
						String val = matcher.group(j+1);
						String stripped = StringUtil.stripNonNumeric(val);
						if (stripped.length()>45) stripped = stripped.substring(0, 44);
						String oldVal = extracted.get(values[j]);
						if (oldVal==null) {
							extracted.put(values[j], stripped);
						}
					}
				}
			}
		}
		return extracted;
	}

	/**
	 * Given a match value and numeric content, check to see if any phone numbers contain the extracted numbers.
	 * Remove bad phone numbers from the array.
	 */
	static int BAD_PHONE_COUNT = 0;
	private static boolean checkBadPhones(String val, String stripped, ArrayList<String> phones, int patternNo, String id) {
		if (phones==null) return false;
		if (stripped.length()<5) return false;
		int toDelete = -1;
		for (int i=0; i<phones.size(); i++) {
			String phone = phones.get(i);
			boolean phoneLonger = phone.length()>stripped.length();
			if ( (phoneLonger&&phone.contains(stripped)) || 
					((!phoneLonger)&&stripped.contains(phone)) ) {
//				System.out.println((BAD_PHONE_COUNT++) + ";" + id + ";" + patternNo + ";(" + val + ");(" + stripped + ");(" + phone + ")");
				toDelete = i;
				break;
			}
		}
		if (toDelete>=0) {
			phones.remove(toDelete);
			return true;
		}
		return false;
	}

	/**
	 * A tester for the regular expressions.
	 */
	public static void test() {
		String str = "<b>SAFE SERVICE <u>ONLY</u></b> <br /> <br /> Hello gentlemen my name is <b>Carlita</b>, <br /> <br /> <b>5'7 130 34DD </b> Spanish beauty <br /> Sexy all natural curves and perky tits and juicy booty . <br /> Green eyes and soft tanned skin with a beautiful face . <br /> Very friendly , down to earth personality . <br /> <br /> <b>100% REAL &amp; RECENT PICTURES</b> <br /> <br /> Don't miss out ! Call to make an appointment . <br /> Let me show you my freaky side , I can't wait to see you ! xo <br /> <br /> Clean &amp; Unrushed . <br /> <br /> <u>No blocked calls please</u> <br /> (416)889-0363 <b>Carlita</b>. <br /> <i>Incall and Outcalls</i>";
		str = str.replaceAll("&quot;", "\"");
		str = str.replaceAll("&amp;", "\"");

		Pattern patterns[] = new Pattern[EXTRACTORS.length];
		for (int i=0; i<EXTRACTORS.length; i++) {
			patterns[i] = Pattern.compile(EXTRACTORS[i].regex);
		}
		HashMap<String, String> extracted = extractValues(patterns, str, null, null);

		for (Entry<String,String> e:extracted.entrySet()) {
			System.out.println(e.getKey() + ":" + e.getValue());
		}
	}

	/**
	 * Read the database, extract the data and write the results.
	 */
	private static void process() {
		System.out.println("Calculating extracted data...");
		TableDB db = TableDB.getInstance();
		HashMap<String,HashMap<String, String>> extractedData = extractData(db);
		
		initTable();
		System.out.println("Writing extracted results...");
		insertExtracted(db, extractedData);
	}

	/**
	 * The main function to initialize the database, log timing and execute the process.
	 */
	public static void main(String[] args) {
//		test();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin RE extraction...");
		long start = System.currentTimeMillis();

		ScriptDBInit.initDB(args);
		process();

		long end = System.currentTimeMillis();
		System.out.println("Done RE extraction in: " + (end-start) + "ms");
	}

}

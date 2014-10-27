package oculus.xdataht.clustering;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import oculus.xdataht.data.TableDB;


public class CleanColumn {

	static String _datasetName = "ads";
	
	static String _name = "xdataht";
	static String _type = "mysql";
	static String _hostname = "localhost";
	static String _port = "3306"; 
	static String _user = "root";
	static String _pass = "admin";

	static HashMap<String,HashSet<String>> _badValues = new HashMap<String,HashSet<String>>();
	
	private static void handlePropertyFileLine(String line) {
		String[] pieces = line.split("=");
		if (pieces.length == 2) {
			if (pieces[0].equals("db_type")) {
				
			} else if (pieces[0].equals("db_hostname")) {
				_hostname = pieces[1];
			} else if (pieces[0].equals("db_port")) {
				_port = pieces[1];
			} else if (pieces[0].equals("db_user")) {
				_user = pieces[1];
			} else if (pieces[0].equals("db_pass")) {
				_pass = pieces[1];
			} else  if (pieces[0].equals("db_name")) {
				_name = pieces[1];
			} else  if (pieces[0].equals("dataset")) {
				_datasetName = pieces[1];
			} else  if (pieces[0].equals("cleancolumn")) {
				String[] vals = pieces[1].split(",");
				readBadVals(vals[0],vals[1]);
			} else  {
				System.out.println("Unknown property: " + pieces[0]);
			}
		} else {
			System.out.println("Malformed property line: " + line);
		}
	}
	
	private static void readBadVals(String column, String file) {
		HashSet<String> badValues = new HashSet<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line;

			while ((line = br.readLine()) != null) {
				badValues.add(line.trim().toLowerCase());
			}
			br.close();

		} catch (FileNotFoundException e) {
			System.out.println("File: " + file + " not found.");
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		_badValues.put(column, badValues);
	}

	public static void initDB(String name, String type, String hostname, String port, 
			String user, String pass) {
		TableDB.getInstance(name, type, hostname, port, user, pass, "");
	}

	public static void filterColumn(String column) {
		Connection conn = TableDB.getInstance().open();
		int minID = 0;
		int maxID = 1000;
		int rows = 0;
		long start = System.currentTimeMillis();
		System.out.println("Cleaning column <" + column + ">");
		do {
			rows = updateRange(conn, minID, maxID, column);
			minID = maxID + 1;
			maxID += 1000;
		} while (rows>0);
		long end = System.currentTimeMillis();
		System.out.println("Done filtering column <" + column + "> in: " + (end-start) + "ms");
		
		TableDB.getInstance().close();
	}
	
	private static int updateRange(Connection conn, int minID, int maxID, String column) {
		HashMap<String,String> updates = new HashMap<String,String>();
		int count = 0;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT id," + column + " FROM "
					+ _datasetName + " WHERE id<='" + maxID + "' && id>='" + minID + "'");
			while (rs.next()) {
				count++;
				String id = rs.getString("id");
				String valueStr = rs.getString(column);
				if (valueStr==null) continue;
				String[] vals = valueStr.split(",");
				ArrayList<String> goodVals = new ArrayList<String>();
				boolean badVals = false;
				for (String val:vals) {
					if (_badValues.get(column).contains(val.toLowerCase())) badVals = true;
					else goodVals.add(val);
				}
				if (badVals) {
					String newValStr = "";
					boolean isFirst = true;
					for (String goodVal:goodVals) {
						if (isFirst) isFirst = false;
						else newValStr += ",";
						newValStr += goodVal;
					}
					updates.put(id, newValStr);
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

		for (Map.Entry<String,String> entry:updates.entrySet()) {
			TableDB.getInstance().tryStatement(conn, "update " + _datasetName + " set " + column + "='" + entry.getValue() +
					"' where id='" + entry.getKey() + "'");
		}

//		System.out.println("Processing: (" + minID + "," + maxID + ") updated " + updates.size() + " of " + count);
		
		return count;
	}
	
	public static void checkWebsites() {
		Connection conn = TableDB.getInstance().open();
		int minID = 0;
		int maxID = 1000;
		int rows = 0;
		HashMap<String, Integer> badVals = new HashMap<String, Integer>();
		do {
			rows = checkRange(conn, minID, maxID, "websites", badVals);
			minID = maxID + 1;
			maxID += 1000;
		} while (rows>0);
		for (Map.Entry<String,Integer> badVal:badVals.entrySet()) {
			if (badVal.getValue()>1)
				System.out.println(badVal.getValue() + ": " + badVal.getKey());
		}
		
		TableDB.getInstance().close();
	}
	
	private static int checkRange(Connection conn, int minID, int maxID, String column, HashMap<String, Integer> badVals) {
		int count = 0;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT id," + column + " FROM "
					+ _datasetName + " WHERE id<='" + maxID + "' && id>='" + minID + "'");
			while (rs.next()) {
				count++;
				String valueStr = rs.getString(column);
				if (valueStr==null) continue;
				String[] vals = valueStr.split(",");
				for (String val:vals) {
					if (val==null || val.length()<3) continue;
					if (!val.startsWith("http")) {
						Integer c = badVals.get(val);
						if (c!=null) badVals.put(val, c.intValue()+1);
						else badVals.put(val, 1);
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
		return count;
	}

	public static void main(String[] args) {
		if (args.length > 0) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(args[0]));
				String line;

				while ((line = br.readLine()) != null) {
					handlePropertyFileLine(line.trim());
				}
				br.close();

			} catch (FileNotFoundException e) {
				System.out.println("File: " + args[0] + " not found.");
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}
		initDB(_name, _type, _hostname, _port, _user, _pass);
		for (String column:_badValues.keySet()) {
			filterColumn(column);
		}
	}
}

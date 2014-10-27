package oculus.xdataht.preprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.TableDB;

import com.oculusinfo.ml.feature.string.distance.EditDistance;

public class AdLinker {
	
	private static final String AD_TABLE = "backpage_ads";
	private static final String LINK_COLUMN = "location";

	private static String localdb_type = "mysql";
	private static String localdb_hostname = "localhost";
	private static String localdb_port = "3306";
	private static String localdb_user = "root";
	private static String localdb_password = "admin";
	private static String localdb_name = "xdataht";

	private static class Link {
		private int id1;
		private int id2;
		private String type;
		private String details;
		public Link(int id1, int id2, String type, String details) {
			this.id1 = id1;
			this.id2 = id2;
			this.type = type;
			this.details = details;
		}
		
	}
	
	public static void createLinkTable(TableDB db, Connection localConn, String dataset) { 
		try {
			if (!db.tableExists(dataset+"_links")) {
				db.tryStatement(localConn, "CREATE TABLE " + dataset + "_links (" +
					  "id int not null auto_increment," +
					  "ad_id1 int," +
					  "ad_id2 int," +
					  "link_type varchar(16)," +
					  "details varchar(16)," +
					  "primary key (id))");
			} else {
				db.tryStatement(localConn, "DELETE FROM " + dataset + "_links");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addLinks2(TableDB db, Connection localConn, String dataset, ArrayList<Link> links) {
		Statement stmt = null;
		try {
			stmt = localConn.createStatement();
			String sqlStr = "INSERT INTO " + dataset + "_links (ad_id1,ad_id2,link_type,details)" +
				"values ";
			boolean isFirst = true;
			for (Link link:links) {
				if (isFirst) isFirst = false;
				else sqlStr += ",";
				sqlStr += "('" + link.id1+ "','" + link.id2 + "','" + link.type + "','" + link.details + "')";
			}
			stmt.execute(sqlStr);
		} catch (Exception e) {
			System.out.println("Failed Insert");
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void addLinks(TableDB db, Connection localConn, String dataset, ArrayList<Link> links) {
		PreparedStatement stmt = null;
		try {
			stmt = localConn.prepareStatement("INSERT INTO " + dataset + "_links (ad_id1,ad_id2,link_type,details)" +
				"values (?,?,?,?)");
			for (Link link:links) {
				stmt.setInt(1,link.id1);
				stmt.setInt(2,link.id2);
				stmt.setString(3,link.type);
				stmt.setString(4,link.details);
				stmt.executeUpdate();
			}
		} catch (Exception e) {
			System.out.println("Failed Insert");
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) {
		TableDB db = TableDB.getInstance(localdb_name, localdb_type, localdb_hostname, localdb_port,localdb_user, localdb_password, "");
		ArrayList<String> nameColumn = new ArrayList<String>();
		nameColumn.add(LINK_COLUMN);
		DataTable adTableNames = db.getDataTableColumns(AD_TABLE, nameColumn);

		Connection conn = db.open();
		createLinkTable(db, conn, AD_TABLE);
		int rowCount = adTableNames.rows.size();
		ArrayList<Link> links = new ArrayList<Link>();
		long start = System.currentTimeMillis();
		for (int i=0; i<rowCount; i++) {
			DataRow row = adTableNames.rows.get(i);
			String rowName = row.get(LINK_COLUMN);
			if (rowName==null) continue;
			rowName = rowName.trim().toLowerCase();
			if (rowName.length()==0) continue;
			int rowid = Integer.parseInt(row.get("id"));
			for (int j=(i+1); j<rowCount; j++) {
				DataRow compare = adTableNames.rows.get(j);
				String compareName = compare.get(LINK_COLUMN);
				if (compareName==null) continue;
				compareName = compareName.trim().toLowerCase();
				if (compareName.length()==0) continue;
				int compareid = Integer.parseInt(compare.get("id"));
				double dist = EditDistance.getNormLevenshteinDistance(rowName, compareName);
				if (dist<0.2) {
					links.add(new Link(rowid, compareid, LINK_COLUMN, Double.toString(Math.round(dist*100))));
					if (links.size()==1000) {
						System.out.println("Processed: " + i + "," + (System.currentTimeMillis()-start) + "ms");
						addLinks2(db, conn, AD_TABLE, links);
						links.clear();
						System.out.println("Added: " + (System.currentTimeMillis()-start) + "ms");
					}
				}
			}
		}
		if (links.size()>0) {
			System.out.println("Done," + (System.currentTimeMillis()-start) + "ms");
			addLinks2(db, conn, AD_TABLE, links);
			links.clear();
			System.out.println("Added: " + (System.currentTimeMillis()-start) + "ms");
		}

		db.close();

	}
}

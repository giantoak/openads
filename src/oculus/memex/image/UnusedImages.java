package oculus.memex.image;

import java.awt.image.BufferedImage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.xdataht.image.ImageHistogram;
import oculus.xdataht.preprocessing.ScriptDBInit;

/**
 * Extract images from ads.
 */
public class UnusedImages {	
	static final public String AD_IMAGES_TABLE = "ads_images";
	public static final int BATCH_SELECT_SIZE = 100000;
	public static final int BATCH_INSERT_SIZE = 2000;

	
	private static void createTable(MemexOculusDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+AD_IMAGES_TABLE+"` (" +
						  "id INT(11) NOT NULL AUTO_INCREMENT," +
						  "ads_id INT(11) NOT NULL," +
						  "images_id INT(11) NOT NULL," +
						  "origin_url VARCHAR(2048)," +
						  "image_url VARCHAR(128)," +
						  "image_hash VARCHAR(128)," +
						  "PRIMARY KEY (id) )";
			DBManager.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initTable(MemexOculusDB db, Connection conn) {
		if (db.tableExists(AD_IMAGES_TABLE)) {
			System.out.println("Clearing table: " + AD_IMAGES_TABLE);
			db.clearTable(AD_IMAGES_TABLE);
		} else {			
			System.out.println("Creating table: " + AD_IMAGES_TABLE);
			createTable(db, conn);
		}
	}
	
	private static class ImageData {
		String origin_url;
		String image_url;
		int ads_id;
		int images_id;
		String image_hash;
		public ImageData(int images_id, int ads_id, String origin_url,
				String image_url, String image_hash) {
			this.images_id = images_id;
			this.ads_id = ads_id;
			this.origin_url = origin_url;
			this.image_url = image_url;
			this.image_hash = image_hash;
		}
	}
	
	/**
	 * For each image in memex_ht.images with ads_id and location not null
	 * hash the image and write to memex_oculus.ads_images
	 */
	public static void getImages(Connection htconn, Connection oculusconn) {
		HashMap<Integer,ImageData> result = new HashMap<Integer,ImageData>();
		String sqlStr = "SELECT id,ads_id,url,location from images where ads_id IS NOT NULL and location IS NOT NULL";
		Statement stmt = null;
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer images_id = rs.getInt("id");
				Integer ads_id = rs.getInt("ads_id");
				String origin_url = rs.getString("url");
				String image_url = rs.getString("location");
				BufferedImage bi = ImageHistogram.getImage(image_url);
				String image_hash = ImageHistogram.getHash(bi);
				ImageData d = new ImageData(images_id, ads_id, origin_url, image_url, image_hash);
				result.put(images_id, d);
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
		insertAdImages(oculusconn, result);
	}
	

	public static void insertAdImages(Connection conn, HashMap<Integer, ImageData> resultMap) {
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO " + AD_IMAGES_TABLE + 
					"(ads_id, images_id, origin_url, image_url, image_hash) VALUES (?,?,?,?,?)");
			int count = 0;
			for (ImageData id:resultMap.values()) {
				pstmt.setInt(0, id.ads_id);
				pstmt.setInt(1, id.images_id);
				pstmt.setString(2, id.origin_url);
				pstmt.setString(3, id.image_url);
				pstmt.setString(4, id.image_hash);
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
	}

	private static void extractImages() {
		System.out.println("Calculating images...");
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		initTable(oculusdb, oculusconn);
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		getImages(htconn, oculusconn);
		oculusdb.close();
		htdb.close();
	}


	public static void main(String[] args) {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin ad image extraction...");
		long start = System.currentTimeMillis();

		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		extractImages();

		long end = System.currentTimeMillis();
		System.out.println("Done ad image extraction in: " + (end-start) + "ms");

	}

}

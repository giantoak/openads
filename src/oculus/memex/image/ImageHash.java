package oculus.memex.image;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import oculus.memex.db.DBManager;
import oculus.memex.db.MemexOculusDB;
import oculus.xdataht.util.TimeLog;

public class ImageHash {

	private static final String IMAGE_HASH_TABLE = "images_hash";
	private static HashMap<Integer,String> IMAGE_HASH_CACHE = null;
	private static HashMap<String,HashSet<Integer>> HASH_TO_IMAGE = null;

	public static void initTable(MemexOculusDB oculusdb) {
		Connection conn = oculusdb.open();
		System.out.println("IMAGE HASH INITIALIZATION");
		if (!oculusdb.tableExists(IMAGE_HASH_TABLE)) {
			String sqlCreate = "CREATE TABLE `" + IMAGE_HASH_TABLE + "` ("
				+ "`id` int(11) NOT NULL AUTO_INCREMENT,"
				+ "`images_id` INT(11) UNSIGNED NOT NULL, "
				+ "`hash` varchar(128) DEFAULT NULL, "
				+ "`bin` int(11) DEFAULT NULL, "
				+ "PRIMARY KEY (`id`), "
				+ "KEY `images_id` (`images_id`))";
			if (DBManager.tryStatement(conn, sqlCreate)) {
				System.out.println("\t" + IMAGE_HASH_TABLE + " table initialized.");
			} else {
				System.out.println("\tError creating " + IMAGE_HASH_TABLE + " table.");
			}
		} else {
			System.out.println("\t" + IMAGE_HASH_TABLE + " table exists.");
		}
		oculusdb.close();
	}
	
	private static void createTable() {
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		if (!oculusdb.tableExists(IMAGE_HASH_TABLE)) {
			String sqlCreate = "CREATE TABLE `" + IMAGE_HASH_TABLE + "` ("
				+ "`id` int(11) NOT NULL AUTO_INCREMENT,"
				+ "`images_id` INT(11) UNSIGNED NOT NULL, "
				+ "`hash` varchar(128), "
				+ "PRIMARY KEY (`id`), "
				+ "KEY `images_id` (`images_id`))";
			if (DBManager.tryStatement(oculusconn, sqlCreate)) {
				System.out.println("\t" + IMAGE_HASH_TABLE + " table initialized.");
			} else {
				System.out.println("\tError creating " + IMAGE_HASH_TABLE + " table.");
			}
		}
		oculusdb.close();
	}

	public static void readImageHashCache() {
		createTable();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		Statement stmt;
		try {
			IMAGE_HASH_CACHE = new HashMap<Integer,String>();
			HASH_TO_IMAGE = new HashMap<String,HashSet<Integer>>();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT images_id,hash FROM " + IMAGE_HASH_TABLE);
			while(rs.next()) {
				Integer id = rs.getInt("images_id");
				String hash = rs.getString("hash");
				cacheImageHash(id, hash);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.close();
	}

	private static void cacheImageHash(Integer id, String hash) {
		IMAGE_HASH_CACHE.put(id, hash);
		HashSet<Integer> allids = HASH_TO_IMAGE.get(hash);
		if (allids==null) {
			allids = new HashSet<Integer>();
			HASH_TO_IMAGE.put(hash, allids);
		}
		allids.add(id);
	}
	
	public static HashMap<Integer,String> getImageHashCache() {
		if (IMAGE_HASH_CACHE==null) {			
			readImageHashCache();
		}
		return IMAGE_HASH_CACHE;
	}
	
	public static byte[] getImage(String urlToRead) {
		URL url;
		HttpURLConnection conn;
		try {
			url = new URL(urlToRead);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			int nRead;
			byte[] data = new byte[16384];
			InputStream is = conn.getInputStream();
			while ((nRead = is.read(data, 0, data.length)) != -1) {
			  buffer.write(data, 0, nRead);
			}
			buffer.flush();
			return buffer.toByteArray();
		} catch (Exception e) {
			System.out.println("Failed to read URL: " + urlToRead);
			e.printStackTrace();
		}
		return null;
	}

	private static String hashImage(String imageurl) {
		byte[] image = getImage(imageurl);
		if (image==null) return null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA");
			return new String(md.digest(image));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private static void writeNewHashes(HashMap<Integer, String> newToCache) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection oculusconn = db.open();
		PreparedStatement pstmt = null;
		try {
			oculusconn.setAutoCommit(false);
			pstmt = oculusconn.prepareStatement("insert into " + IMAGE_HASH_TABLE + "(images_id,hash) values (?,?)");
			for (Map.Entry<Integer,String> entry:newToCache.entrySet()) {
				pstmt.setInt(1,entry.getKey());
				pstmt.setString(2,entry.getValue());
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (pstmt != null) { pstmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
			
			try {
				oculusconn.setAutoCommit(true);
			} catch (SQLException e) {e.printStackTrace();}
		}
		db.close();
	}
	
	public static String getHash(int imageid) {
		return getImageHashCache().get(imageid);
	}

	public static void addImages(HashMap<Integer, String> newImages) {
		TimeLog tl = new TimeLog();
		tl.pushTime("Hashing images: " + newImages.size());
		HashMap<Integer, String> cache = getImageHashCache();
		HashMap<Integer, String> newToCache = new HashMap<Integer,String>();
		for (Map.Entry<Integer,String> e:newImages.entrySet()) {
			Integer imageid = e.getKey();
			String imageurl = e.getValue();
			String hash = cache.get(imageid);
			if (hash==null && imageurl!=null) {
				hash = hashImage(imageurl);
				cacheImageHash(imageid, hash);
				if (hash!=null) newToCache.put(imageid,hash);
			}
		}
		tl.pushTime("Writing new hashes " + newToCache.size());
		if (newToCache.size()>0) writeNewHashes(newToCache);
		tl.popTime();
		tl.popTime();
	}

	public static HashSet<Integer> getIds(String hash) {
		getImageHashCache(); // Make sure we've read from the database
		return HASH_TO_IMAGE.get(hash);
	}
}
package oculus.memex.image;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.imageio.ImageIO;

import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.StringUtil;
import oculus.xdataht.util.TimeLog;

import org.apache.commons.io.IOUtils;

public class ImageHistogramHash {

	private static final int COLOR_DEPTH = 4;
	private static final int COLOR_DIVISOR = 256/COLOR_DEPTH;
	private static final int DISTINCT_COLORS = (int)Math.pow(COLOR_DEPTH,3);
	private static final int COMPARE_THRESHOLD = 10;
	private static final int BATCH_INSERT_SIZE = 1000;

	private static final String IMAGE_HASH_TABLE = "images_hash";
	private static HashMap<Integer,String> IMAGE_HASH_CACHE = null;
	private static HashMap<String,HashSet<Integer>> HASH_TO_IMAGE = null;

	private static HashMap<Integer,HashSet<String>> BIN_TO_HASH = null;
	private static HashMap<String,Integer> HASH_TO_BIN = null;
	private static int MAX_BIN = 0;
	
	private static void createTable() {
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		if (!oculusdb.tableExists(IMAGE_HASH_TABLE)) {
			String sqlCreate = "CREATE TABLE `" + IMAGE_HASH_TABLE + "` ("
				+ "`id` int(11) NOT NULL AUTO_INCREMENT,"
				+ "`images_id` INT(11) UNSIGNED NOT NULL, "
				+ "`hash` varchar(128), "
				+ "bin int(11),"
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
	
	@SuppressWarnings("unused")
	private static void cacheHashedImages() {
		HashMap<Integer,String> urls = new HashMap<Integer,String>();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT images.id,images.location FROM " + IMAGE_HASH_TABLE +
					" INNER JOIN " + ScriptDBInit._htSchema + ".images ON images.id=" + IMAGE_HASH_TABLE + ".images_id");
			while(rs.next()) {
				Integer id = rs.getInt("id");
				String location = rs.getString("location");
				urls.put(id,location);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.close();

		for (Map.Entry<Integer, String> e:urls.entrySet()) {
			Integer imageid = e.getKey();
			String url = e.getValue();
			if (url==null) continue;
			byte[] image = ImageHash.getImage(url);
			File outfile = new File("c:/dev/escortimages/"+imageid+".jpg");
			try {
				FileOutputStream fos = new FileOutputStream(outfile);
				IOUtils.write(image, fos);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
		conn = db.open();
		PreparedStatement pstmt = null;
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("UPDATE " + ScriptDBInit._htSchema + ".images set location=? where id=?");
			int count = 0;
			for (Map.Entry<Integer, String> e:urls.entrySet()) {
				Integer imageid = e.getKey();
				String url = e.getValue();
				if (url==null) continue;
				pstmt.setString(1,"escortimages/"+imageid+".jpg");
				pstmt.setInt(2, imageid);
				pstmt.addBatch();
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
	
	private static void readImageHashCache() {
		createTable();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		Statement stmt;
		try {
			IMAGE_HASH_CACHE = new HashMap<Integer,String>();
			HASH_TO_IMAGE = new HashMap<String,HashSet<Integer>>();
			BIN_TO_HASH = new HashMap<Integer,HashSet<String>>();
			HASH_TO_BIN = new HashMap<String,Integer>();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT images_id,hash,bin FROM " + IMAGE_HASH_TABLE);
			while(rs.next()) {
				Integer id = rs.getInt("images_id");
				String hash = rs.getString("hash");
				Integer bin = rs.getInt("bin");
				if (bin>MAX_BIN) MAX_BIN = bin;
				cacheImageHash(id, hash, bin);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.close();
	}



	private static void cacheImageHash(Integer id, String hash, Integer bin) {
		IMAGE_HASH_CACHE.put(id, hash);
		HashSet<Integer> allids = HASH_TO_IMAGE.get(hash);
		if (allids==null) {
			allids = new HashSet<Integer>();
			HASH_TO_IMAGE.put(hash, allids);
		}
		allids.add(id);
		
		if (bin!=null) {
			HASH_TO_BIN.put(hash, bin);
			HashSet<String> allHashes = BIN_TO_HASH.get(bin);
			if (allHashes==null) {
				allHashes = new HashSet<String>();
				BIN_TO_HASH.put(bin, allHashes);
			}
			allHashes.add(hash);
		} else {
			// Find the best bin
			boolean found = false;
			for (Map.Entry<Integer,HashSet<String>> e:BIN_TO_HASH.entrySet()) {
				Integer cbin = e.getKey();
				HashSet<String> allHashes = e.getValue();
				String chash = allHashes.iterator().next();
				if (compareHashes(chash, hash)) {
					allHashes.add(hash);
					HASH_TO_BIN.put(hash, cbin);
					found = true;
					break;
				}
			}
			if (!found) {
				MAX_BIN++;
				int cbin = MAX_BIN;
				HashSet<String> allHashes = new HashSet<String>();
				allHashes.add(hash);
				BIN_TO_HASH.put(cbin, allHashes);
				HASH_TO_BIN.put(hash, cbin);
			}
		}
	}
	
	@SuppressWarnings("unused")
	private static void rebinHashes() {
		readImageHashCache();
		HASH_TO_BIN.clear();
		BIN_TO_HASH.clear();
		MAX_BIN = 0;
		
		for (Map.Entry<Integer,String> img:IMAGE_HASH_CACHE.entrySet()) {
			Integer id = img.getKey();
			String hash = img.getValue();
			// Find the best bin
			boolean found = false;
			for (Map.Entry<Integer,HashSet<String>> e:BIN_TO_HASH.entrySet()) {
				Integer cbin = e.getKey();
				HashSet<String> allHashes = e.getValue();
				String chash = allHashes.iterator().next();
				if (compareHashes(chash, hash)) {
					allHashes.add(hash);
					HASH_TO_BIN.put(hash, cbin);
					found = true;
					break;
				}
			}
			if (!found) {
				MAX_BIN++;
				int cbin = MAX_BIN;
				HashSet<String> allHashes = new HashSet<String>();
				allHashes.add(hash);
				BIN_TO_HASH.put(cbin, allHashes);
				HASH_TO_BIN.put(hash, cbin);
			}
		}
		updateBins();
		
	}
	
	private static boolean compareHashes(String chash, String hash) {
		if (chash==null || hash==null) return false;
		byte[] cbytes = StringUtil.hexToBytes(chash);
		byte[] bytes = StringUtil.hexToBytes(hash);
		if (cbytes.length!=bytes.length) return false;
		int dif = 0;
		for (int i=0; i<cbytes.length; i++) {
			dif += Math.abs(cbytes[i]-bytes[i]);
		}
		return dif<COMPARE_THRESHOLD;
	}



	private static HashMap<Integer,String> getImageHashCache() {
		if (IMAGE_HASH_CACHE==null) {			
			readImageHashCache();
		}
		return IMAGE_HASH_CACHE;
	}
	
	private static BufferedImage getImageImg(String urlToRead) {
		URL url;
		HttpURLConnection conn;
		try {
			url = new URL(urlToRead);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			return ImageIO.read(conn.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static String histogramHash(String url) {
		BufferedImage img = getImageImg(url);
		if (img==null) return null;
		return histogramHash(img);
	}

	public static String histogramHash(BufferedImage img) {
		Raster raster = img.getData();
		int h = raster.getHeight();
		int w = raster.getWidth();
		int pixels = w*h;
		int[] colors = new int[pixels*3];
		raster.getPixels(0, 0, w, h, colors);
		int[] counts = new int[DISTINCT_COLORS];
		for (int i=0; i<DISTINCT_COLORS; i++) counts[i] = 0;
		for (int i=0; i<w*h; i++) {
			int r = colors[i*3]/COLOR_DIVISOR;
			r = Math.min(r, COLOR_DEPTH-1);
			int g = (colors[i*3+1])/COLOR_DIVISOR;
			g = Math.min(g, COLOR_DEPTH-1);
			int b = (colors[i*3+2])/COLOR_DIVISOR;
			b = Math.min(b, COLOR_DEPTH-1);
			int truncColor = (r*COLOR_DEPTH+g)*COLOR_DEPTH+b;
			counts[truncColor]++;
		}
		byte[] result = new byte[DISTINCT_COLORS];
		for (int i=0; i<DISTINCT_COLORS; i++) {
			int count = (int)Math.ceil((counts[i]*DISTINCT_COLORS)/pixels);
			result[i] = (byte)(count&0xFF);
		}
		return StringUtil.bytesToHex(result);
	}

	
	
	private static void writeNewHashes(HashMap<Integer, String> newToCache) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection oculusconn = db.open();
		PreparedStatement pstmt = null;
		try {
			oculusconn.setAutoCommit(false);
			pstmt = oculusconn.prepareStatement("insert into " + IMAGE_HASH_TABLE + "(images_id,hash,bin) values (?,?,?)");
			for (Map.Entry<Integer,String> entry:newToCache.entrySet()) {
				Integer imageid = entry.getKey();
				String hash = entry.getValue();
				pstmt.setInt(1,imageid);
				pstmt.setString(2,hash);
				pstmt.setInt(3, HASH_TO_BIN.get(hash));
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
	
	private static void updateBins() {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection oculusconn = db.open();
		PreparedStatement pstmt = null;
		try {
			oculusconn.setAutoCommit(false);
			pstmt = oculusconn.prepareStatement("update " + IMAGE_HASH_TABLE + " set bin=? where images_id=?");
			for (Map.Entry<Integer,String> entry:IMAGE_HASH_CACHE.entrySet()) {
				Integer imageid = entry.getKey();
				String hash = entry.getValue();
				pstmt.setInt(1, HASH_TO_BIN.get(hash));
				pstmt.setInt(2,imageid);
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
//				hash = hashImage(imageurl);
				hash = histogramHash(imageurl);
				if (hash!=null) {
					cacheImageHash(imageid, hash, null);
					newToCache.put(imageid,hash);
				}
			}
		}
		tl.pushTime("Writing new hashes " + newToCache.size());
		if (newToCache.size()>0) writeNewHashes(newToCache);
		tl.popTime();
		tl.popTime();
	}

	public static String getHash(int imageid) {
//		return getImageHashCache().get(imageid);
		String hash = getImageHashCache().get(imageid);
		if (hash==null) return null;
		return ""+HASH_TO_BIN.get(hash);
	}

	public static HashSet<Integer> getIds(String hash) {
		getImageHashCache(); // Make sure we've read from the database
		HashSet<Integer> result = new HashSet<Integer>();
		try {
			int bin = Integer.parseInt(hash);
			HashSet<String> hashes = BIN_TO_HASH.get(bin);
			for (String chash:hashes) {
				HashSet<Integer> images = HASH_TO_IMAGE.get(chash);
				if (images!=null) result.addAll(images);
			}
		} catch (Exception e) {
			HashSet<Integer> images = HASH_TO_IMAGE.get(hash);
			if (images!=null) result.addAll(images);
		}
		return result;
	}

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin image caching...");
		long start = System.currentTimeMillis();
		ScriptDBInit.readArgs(args);
		MemexOculusDB.getInstance(ScriptDBInit._oculusSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
		MemexHTDB.getInstance(ScriptDBInit._htSchema, ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass);
//		cacheHashedImages();
		rebinHashes();
		long end = System.currentTimeMillis();
		System.out.println("Done image caching: " + (end-start) + "ms");
		
	}


}

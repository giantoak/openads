/**
 * Copyright (c) 2013 Oculus Info Inc.
 * http://www.oculusinfo.com/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package oculus.xdataht.ist;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ISTDB {
	
	private static String localdb_type = "mysql";
	private static String localdb_hostname = "localhost";
	private static String localdb_port = "3306";
	private static String localdb_user = "root";
	private static String localdb_password = "admin";
	private static String localdb_name = "xdataht";

	private static String remotedb_type = "mysql";
	private static String remotedb_hostname = "roxy-db.istresearch.com";
	private static String remotedb_port = "3306";
	private static String remotedb_user = "oculus";
	private static String remotedb_password = "RrzGuS6s3GaUZ3yB";
	private static String remotedb_name = "roxy_ui";
	

	private static boolean _driverLoaded = false;
	
	public static void main(String[] args) {
		createWideTable();
		copyWide();
//		createBackpageIncoming();
//		copyBackpageIncoming();
	}
	
	public static void createWideTable() {
		Connection localConn = null;
		initDriver();
		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			if (!tableExists(localConn, "ads")) {
				tryStatement(localConn, "CREATE TABLE ads (" +
				  "id int(11) NOT NULL AUTO_INCREMENT," +
				  "parent_id int(11) NOT NULL," +
				  "source varchar(64) NOT NULL," +
				  "adid int(11) DEFAULT NULL," +
				  "body text," +
				  "title varchar(1024) DEFAULT NULL," +
				  "keywords varchar(1024) DEFAULT NULL," +
				  "description text," +
				  "images text," +
				  "images_location text," +
				  "image_alt text," +
				  "phone_numbers varchar(200) DEFAULT NULL," +
				  "location varchar(45) DEFAULT NULL," +
				  "websites varchar(1024) DEFAULT NULL," +
				  "age varchar(3) DEFAULT NULL," +
				  "region varchar(256) DEFAULT NULL," +
				  "email varchar(200) DEFAULT NULL," +
				  "otherads varchar(128) DEFAULT NULL," +
				  "ethnicity varchar(256) DEFAULT NULL," +
				  "service varchar(256) DEFAULT NULL," +
				  "views int(8) DEFAULT NULL," +
				  "availability varchar(256) DEFAULT NULL," +
				  "name varchar(512) DEFAULT NULL," +
				  "eye_color varchar(32) DEFAULT NULL," +
				  "hair_color varchar(32) DEFAULT NULL," +
				  "build varchar(32) DEFAULT NULL," +
				  "height varchar(32) DEFAULT NULL," +
				  "bust varchar(8) DEFAULT NULL," +
				  "cup varchar(8) DEFAULT NULL," +
				  "kitty varchar(32) DEFAULT NULL," +
				  "rate varchar(1024) DEFAULT NULL," +
				  "incall varchar(1024) DEFAULT NULL," +
				  "outcall varchar(1024) DEFAULT NULL," +
				  "screening varchar(8) DEFAULT NULL," +
				  "premier varchar(32) DEFAULT NULL," +
				  "post_timestamp varchar(45) DEFAULT NULL," +
				  "PRIMARY KEY (id)," +
				  "UNIQUE KEY parent_id_source_index (parent_id,source)," +
				  "KEY backpage_ads_locationidx (location)," +
				  "KEY backpage_ads_emailidx (email)," +
				  "KEY backpage_ads_phoneidx (phone_numbers))");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static void createBackpageTable() {
		Connection localConn = null;
		initDriver();
		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			if (!tableExists(localConn, "scraped_backpage_ads")) {
				tryStatement(localConn, "CREATE TABLE scraped_backpage_ads (" +
					  "`id` int(11) NOT NULL AUTO_INCREMENT," +
					  "`adid` int(11) NOT NULL," +
					  "`incomingid` int(11) DEFAULT NULL," +
					  "`title` text NOT NULL," +
					  "`body` text NOT NULL," +
					  "`age` int(3) DEFAULT NULL," +
					  "`location` text NOT NULL," +
					  "`region` text NOT NULL," +
					  "`url` text NOT NULL," +
					  "`otherads` text," +
					  "`post_time` text NOT NULL," +
					  "`first_timestamp` datetime NOT NULL," +
					  "`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
					  "PRIMARY KEY (`id`)," +
					  "KEY `adid_index` (`adid`)," +
					  "KEY `incomingid_index` (`incomingid`)" +
					  ")");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void createBackpageIncoming() {
		Connection localConn = null;
		initDriver();
		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			if (!tableExists(localConn, "backpage_incoming")) {
				tryStatement(localConn, 
						"CREATE TABLE `backpage_incoming` (" +
						  "`id` int(11) NOT NULL AUTO_INCREMENT," +
						  "`url` text NOT NULL," +
						  "`status` text NOT NULL," +
						  "`headers` text NOT NULL," +
						  "`flags` text," +
						  "`body` text NOT NULL," +
						  "`imported` int(1) NOT NULL DEFAULT '0'," +
						  "`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
						  "PRIMARY KEY (`id`)," +
						  "KEY `url` (`url`(255))," +
						  "KEY `imported_iundex` (`imported`))");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void copyBackpage() {
		Connection localConn = null;
		Connection remoteConn = null;

		initDriver();

		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			remoteConn = getConnection(remotedb_user, remotedb_password, remotedb_type, remotedb_hostname, remotedb_port, remotedb_name);
			selectAndInsert(localConn, remoteConn);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				if (remoteConn!=null) remoteConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void insertBackpage(Connection localConn, ResultSet rs) {
		PreparedStatement stmt = null;
		try {
			stmt = localConn.prepareStatement("INSERT INTO scraped_backpage_ads(id,adid,incomingid,title,body,age,location,region,url,otherads,post_time,first_timestamp,timestamp)" +
					"values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
			stmt.setInt(1, rs.getInt("id"));
			stmt.setInt(2, rs.getInt("adid"));
			stmt.setInt(3, rs.getInt("incomingid"));
			stmt.setString(4, rs.getString("title"));
			stmt.setString(5, rs.getString("body"));
			stmt.setInt(6, rs.getInt("age"));
			stmt.setString(7, rs.getString("location"));
			stmt.setString(8, rs.getString("region"));
			stmt.setString(9, rs.getString("url"));
			stmt.setString(10, rs.getString("otherads"));
			stmt.setString(11, rs.getString("post_time"));
			stmt.setString(12, rs.getString("first_timestamp"));
			stmt.setString(13, rs.getString("timestamp"));
			
			stmt.executeUpdate();
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
	
	private static void selectAndInsert(Connection localConn, Connection remoteConn) {
		Statement stmt = null;
		try {
			stmt = remoteConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM scraped_backpage_ads");
			int count = 0;
			while (rs.next()) {
				if ((++count)%1000==0) System.out.println(count);
				insertBackpage(localConn, rs);
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
	
	public static void copyBackpageIncoming() {
		Connection localConn = null;
		Connection remoteConn = null;

		initDriver();

		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			remoteConn = getConnection(remotedb_user, remotedb_password, remotedb_type, remotedb_hostname, remotedb_port, remotedb_name);
			selectAndInsertIncoming(localConn, remoteConn);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				if (remoteConn!=null) remoteConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void copyWide() {
		Connection localConn = null;
		Connection remoteConn = null;

		initDriver();

		try {
			localConn = getConnection(localdb_user, localdb_password, localdb_type, localdb_hostname, localdb_port, localdb_name);
			remoteConn = getConnection(remotedb_user, remotedb_password, remotedb_type, remotedb_hostname, remotedb_port, remotedb_name);
			selectAndInsertWide(localConn, remoteConn);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (localConn!=null) localConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				if (remoteConn!=null) remoteConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	
	private static void insertBackpageIncoming(Connection localConn, ResultSet rs) {
		PreparedStatement stmt = null;
		try {
			  
			stmt = localConn.prepareStatement("INSERT INTO backpage_incoming(id,url,status,headers,flags,body,imported,timestamp)" +
					"values (?,?,?,?,?,?,?,?)");
			stmt.setInt(1, rs.getInt("id"));
			stmt.setString(2, rs.getString("url"));
			stmt.setString(3, rs.getString("status"));
			stmt.setString(4, rs.getString("headers"));
			stmt.setString(5, rs.getString("flags"));
			stmt.setString(6, rs.getString("body"));
			stmt.setInt(7, rs.getInt("imported"));
			stmt.setString(8, rs.getString("timestamp"));
			
			stmt.executeUpdate();
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
	
	private static void insertWide(Connection localConn, ResultSet rs) {
		PreparedStatement stmt = null;
		try {
			  
			stmt = localConn.prepareStatement("INSERT INTO ads(id,parent_id,source,adid,body,title,keywords,description,images,images_location,image_alt,phone_numbers,location,websites,age,region,email,otherads,ethnicity,service,views,availability,name,eye_color,hair_color,build,height,bust,cup,kitty,rate,incall,outcall,screening,premier,post_timestamp) " +
					"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

			stmt.setInt(1, rs.getInt("id"));
			stmt.setInt(2, rs.getInt("parent_id"));
			stmt.setString(3, rs.getString("source"));
			stmt.setInt(4, rs.getInt("adid"));
			stmt.setString(5, rs.getString("body"));
			stmt.setString(6, rs.getString("title"));
			stmt.setString(7, rs.getString("keywords"));
			stmt.setString(8, rs.getString("description"));
			stmt.setString(9, rs.getString("images"));
			stmt.setString(10, rs.getString("images_location"));
			stmt.setString(11, rs.getString("image_alt"));
			stmt.setString(12, rs.getString("phone_numbers"));
			stmt.setString(13, rs.getString("location"));
			stmt.setString(14, rs.getString("websites"));
			stmt.setString(15, rs.getString("age"));
			stmt.setString(16, rs.getString("region"));
			stmt.setString(17, rs.getString("email"));
			stmt.setString(18, rs.getString("otherads"));
			stmt.setString(19, rs.getString("ethnicity"));
			stmt.setString(20, rs.getString("service"));
			stmt.setInt(21, rs.getInt("views"));
			stmt.setString(22, rs.getString("availability"));
			stmt.setString(23, rs.getString("name"));
			stmt.setString(24, rs.getString("eye_color"));
			stmt.setString(25, rs.getString("hair_color"));
			stmt.setString(26, rs.getString("build"));
			stmt.setString(27, rs.getString("height"));
			stmt.setString(28, rs.getString("bust"));
			stmt.setString(29, rs.getString("cup"));
			stmt.setString(30, rs.getString("kitty"));
			stmt.setString(31, rs.getString("rate"));
			stmt.setString(32, rs.getString("incall"));
			stmt.setString(33, rs.getString("outcall"));
			stmt.setString(34, rs.getString("screening"));
			stmt.setString(35, rs.getString("premier"));
			stmt.setString(36, rs.getString("post_timestamp"));
			
			stmt.executeUpdate();
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
	
	private static void selectAndInsertIncoming(Connection localConn, Connection remoteConn) {
		Statement stmt = null;
		try {
			stmt = remoteConn.createStatement();
			int total = 0;
			boolean done = false;
			ResultSet rs = stmt.executeQuery("SELECT * FROM backpage_incoming where id<=" + 1000);
			while (!done) {
				int count = 0;
				while (rs.next()) {
					count++;
					insertBackpageIncoming(localConn, rs);
				}
				System.out.println((total+count)+"");
				total += 1000;
				if (count==0) { 
					done = true; 
				} else {
					rs = stmt.executeQuery("SELECT * FROM backpage_incoming where id>" + total + " and id<=" + (total+1000));
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
	
	private static void selectAndInsertWide(Connection localConn, Connection remoteConn) {
		Statement stmt = null;
		try {
			stmt = remoteConn.createStatement();
			int total = 0;
			boolean done = false;
			ResultSet rs = stmt.executeQuery("SELECT * FROM ads where id<=" + 1000);
			while (!done) {
				int count = 0;
				while (rs.next()) {
					count++;
					insertWide(localConn, rs);
				}
				System.out.println((total+count)+"");
				total += 1000;
				if (count==0) { 
					done = true; 
				} else {
					rs = stmt.executeQuery("SELECT * FROM ads where id>" + total + " and id<=" + (total+1000));
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
	
	
	private static void initDriver() {
		if (_driverLoaded) return;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			_driverLoaded = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection(String database_user, String database_password, 
			String database_type, String database_hostname, String database_port, String database_name) throws Exception {
		Properties connectionProps = new Properties();
		connectionProps.put("user", database_user);
		connectionProps.put("password", database_password);
		
		Connection conn = DriverManager.getConnection("jdbc:" + database_type + "://" + 
					database_hostname +	":" + database_port + "/" + database_name, connectionProps);
		return conn;
	}

	public static boolean tryStatement(Connection conn, String sqlStr) {
		boolean success = false;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sqlStr);
			success = true;
		} catch (Exception e) {
			System.out.println("Failed sql: " + sqlStr);
			e.printStackTrace();
			success = false;
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return success;
	}
	
	public static boolean tableExists(Connection conn, String table) {
		ResultSet tables = null;
		try {
			DatabaseMetaData dbm = conn.getMetaData();
			tables = dbm.getTables(null, null, table, null);
			return tables.next();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (tables!=null) {
				try {
					tables.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return false; 
	}

}

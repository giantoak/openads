package oculus.xdataht.geocode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oculus.xdataht.data.CsvParser;
import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.Pair;

public class Geocoder {
	
	private static String localdb_type = "mysql";
	private static String localdb_hostname = "localhost";
	private static String localdb_port = "3306";
	private static String localdb_user = "root";
	private static String localdb_password = "admin";
	private static String localdb_name = "xdataht";
	
	private static ArrayList<String> columns;
	private static ArrayList<DataRow> rows;
	
	private static String worldCitiesTextFile = "C:/Users/cdickson/Desktop/worldcities/worldcitiespop.txt";
	private static String stateAndProvincesTextFile = "C:/Users/cdickson/Desktop/worldcities/statesandprovinces.txt";

	private static String CITIES_TABLE_NAME = "world_cities";
	private static String STATES_TABLE_NAME = "states_provinces";
	
	private static String STATES_NAME_COLUMN = "name";
	private static String CITIES_NAME_COLUMN = "city";
	private static String CITIES_ALPHA_NAME_COLUMN = "cityalpha";
	
	private static Pair<Float,Float> geocodeAlphaOnly(TableDB db, Connection localConn, String location) {
		return geocode(db, localConn, location, true);
	}
	
	public static ArrayList<Pair<Float,Float>> geocodeAlphaOnly(TableDB db, Connection localConn, ArrayList<String> locations) {
		return geocode(db, localConn, locations, true);
	}
	
	public static Pair<Float,Float> geocode(TableDB db, Connection localConn, String location) {
		return geocode(db, localConn, location, true);
	}
	
	public static ArrayList<Pair<Float,Float>> geocode(TableDB db, Connection localConn, ArrayList<String> locations) {
		return geocode(db, localConn, locations, false);
	}
	
	public static boolean isProvinceOrState(TableDB db, Connection localConn, String location) {
		if (!db.tableExists(STATES_TABLE_NAME)) {
			return false;
		}
		
		String locStr = location.replaceAll("'", "\\\\'");
		locStr = locStr.replaceAll(" ", "");
		String sqlStr = "SELECT * FROM "+STATES_TABLE_NAME+" WHERE namealpha like'" + locStr + "'";
		Statement stmt = null;
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);  
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			System.out.println("Failed state select: " + sqlStr);
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				System.out.println("Failed to find state: " + location);
				e.printStackTrace();
			}
		}
		return false;
		
	}

	private static Pair<Float,Float> geocode(TableDB db, Connection localConn, String location, boolean alphaOnly) {
		if (location==null) return null;
		String[] places = location.split(",");
		
		// Look at names in order trying to geocode cities, then states
		for (int i=0; i<places.length; i++) {
			String place = places[i];
			place = place.trim();
			if ((places.length-i>2) || (!isProvinceOrState(db, localConn, place))) {
				Pair<Float,Float> latlon = null;
				if ((places.length-i)>1) {
					String state = places[i+1].trim();
					latlon = geocodeCity(db, localConn, place, state, alphaOnly);
				} else {
					latlon = geocodeCity(db, localConn, place, alphaOnly);
				}
				if (latlon != null && latlon.getFirst() != null && latlon.getSecond() != null) {
					return latlon;
				}
			} else {
				return geocodeState(db, localConn, places[i].trim(), alphaOnly);
			}
		}
		
		return null;
	}
	
	private static Pair<Float,Float> geocodeState(TableDB db, Connection localConn, String location, boolean alphaOnly) {
		if (db.tableExists(STATES_TABLE_NAME)) {
			return geocode(db, localConn, location, STATES_TABLE_NAME, alphaOnly ? STATES_NAME_COLUMN + "alpha" : STATES_NAME_COLUMN);
		} else {
			return null;
		}
	}
	
	private static Pair<Float,Float> geocodeCity(TableDB db, Connection localConn, String location, boolean alphaOnly) {
		return geocode(db, localConn, location, CITIES_TABLE_NAME, alphaOnly ? CITIES_ALPHA_NAME_COLUMN:CITIES_NAME_COLUMN);
	}
	
	private static Pair<Float,Float> geocodeCity(TableDB db, Connection localConn, String location, String state, boolean alphaOnly) {
		return geocodeCityState(db, localConn, location, state);
	}
		private static Pair<Float,Float> geocode(TableDB db, Connection localConn, String location, String tableName, String columnName) {
		Statement stmt = null;
		ResultSet rs = null;
		String sqlStr = null;
		try {
			stmt = localConn.createStatement();
			String locStr = location.replaceAll("'", "\\\\'");
			locStr = locStr.replaceAll(" ", "");
			if (tableName.equals(CITIES_TABLE_NAME)) {
				sqlStr = "SELECT lat,lon FROM "+tableName+" WHERE " + columnName + " like '" + locStr + "' ORDER BY population DESC LIMIT 0,1";
			} else if (tableName.equals(STATES_TABLE_NAME)) {
				sqlStr = "SELECT lat,lon FROM "+tableName+" WHERE " + columnName + " like '" + locStr + "'";
			}
			if (sqlStr!=null) {
				rs = stmt.executeQuery(sqlStr);
			}
			if (rs != null && rs.next()) {
				float lat = rs.getFloat(1);
				float lon = rs.getFloat(2);
				return new Pair<Float,Float>(lat,lon);
			}
		} catch (Exception e) {
			System.out.println("Failed geocode SQL: " + sqlStr);
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	private static Pair<Float,Float> geocodeCityState(TableDB db, Connection localConn, String city, String state) {
		Statement stmt = null;
		ResultSet rs = null;
		String sqlStr = null;
		Pair<Float,Float> result = null;
		try {
			stmt = localConn.createStatement();
			String locStr = city.replaceAll("'", "\\\\'");
			locStr = locStr.replaceAll(" ", "");
			sqlStr = "SELECT lat,lon,region FROM "+CITIES_TABLE_NAME+" WHERE " + CITIES_ALPHA_NAME_COLUMN + " like '" + locStr + "' ORDER BY population DESC";
			if (sqlStr!=null) {
				rs = stmt.executeQuery(sqlStr);
			}
			if (rs != null) {
				float firstLat = -999;
				float firstLon = 0;
				while (rs.next()) {
					float lat = rs.getFloat(1);
					float lon = rs.getFloat(2);
					if (firstLat==-999) {
						firstLat = lat;
						firstLon = lon;
					}
					String region = rs.getString("region");
					if (region.compareToIgnoreCase(state)==0) {
						result = new Pair<Float,Float>(lat,lon);
						break;
					}
				}
				if (result==null) {
					result = new Pair<Float,Float>(firstLat,firstLon);
				}
			}
		} catch (Exception e) {
			System.out.println("Failed geocode SQL: " + sqlStr);
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

	public static ArrayList<Pair<Float,Float>> geocode(TableDB db, Connection localConn, ArrayList<String> locations, boolean alphaOnly) {
		ArrayList<Pair<Float,Float>> result = new ArrayList<Pair<Float,Float>>();
		for (String location : locations) {
			result.add(geocode(db, localConn, location));
		}
		return result;
	}
	
	private static void insertCity(Connection localConn, DataRow row) {
		PreparedStatement stmt = null;
		try {
			stmt = localConn.prepareStatement("INSERT INTO " + CITIES_TABLE_NAME + "(country,city,accentcity,region,population,lat,lon)" +
					"values (?,?,?,?,?,?,?)");
			stmt.setString(1, row.get("Country"));
			stmt.setString(2, row.get("City"));
			stmt.setString(3, row.get("AccentCity"));
			stmt.setString(4, row.get("Region"));
			
			String popString = row.get("Population");
			Integer pop = (popString == null || popString.equalsIgnoreCase("null") || popString.equalsIgnoreCase("")) ? 0 : Integer.parseInt(popString); 

			String latString = row.get("Latitude");
			Float lat = (latString == null || latString.equalsIgnoreCase("null") || latString.equalsIgnoreCase("")) ? 0 : Float.parseFloat(latString); 

			String longString = row.get("Longitude");
			Float lon = (longString == null || longString.equalsIgnoreCase("null")|| longString.equalsIgnoreCase("")) ? 0 : Float.parseFloat(longString); 

			stmt.setInt(5, pop);
			
			stmt.setFloat(6, lat);
			stmt.setFloat(7, lon);
		
			
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
	
	public static void insertState(Connection localConn, DataRow row) {
		PreparedStatement stmt = null;
		try {
			stmt = localConn.prepareStatement("INSERT INTO " + STATES_TABLE_NAME + "(country,abbreviation,name,namealpha,lat,lon)" +
					"values (?,?,?,?,?,?)");
			stmt.setString(1, row.get("country").toLowerCase());
			stmt.setString(2, row.get("abbreviation").toLowerCase());
			stmt.setString(3, row.get("name").toLowerCase());
			
			
			String nameAlpha = row.get("name");
			nameAlpha = nameAlpha.replace(" ", "").toLowerCase();
			stmt.setString(4, nameAlpha);

			String latString = row.get("lat");
			Float lat = (latString == null || latString.equalsIgnoreCase("null") || latString.equalsIgnoreCase("")) ? 0 : Float.parseFloat(latString); 

			String longString = row.get("lon");
			Float lon = (longString == null || longString.equalsIgnoreCase("null")|| longString.equalsIgnoreCase("")) ? 0 : Float.parseFloat(longString); 
			
			stmt.setFloat(5, lat);
			stmt.setFloat(6, lon);
		
			
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
	
	public static void createStatesAndProvincesTable(TableDB db, Connection localConn) { 
		try {
			if (!db.tableExists(STATES_TABLE_NAME)) {
				db.tryStatement(localConn, "CREATE TABLE " + STATES_TABLE_NAME + " (" +
					  "`country` varchar(2)," +
					  "`abbreviation` varchar(2)," +
					  "`name` varchar(250) DEFAULT NULL," +
					  "`namealpha` varchar(250) DEFAULT NULL," +
					  "`lat` float(10,6) DEFAULT NULL," +
					  "`lon` float(10,6) DEFAULT NULL" +
					  ")");
				
				db.tryStatement(localConn, "CREATE INDEX statealpha_idx ON " + STATES_TABLE_NAME + " (namealpha)");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createWorldCitiesTable(TableDB db, Connection localConn) { 
		try {
			if (!db.tableExists(CITIES_TABLE_NAME)) {
				db.tryStatement(localConn, "CREATE TABLE " + CITIES_TABLE_NAME + " (" +
					  "`country` varchar(2)," +
					  "`city` varchar(250) DEFAULT NULL," +
					  "`accentcity` varchar(250) DEFAULT NULL," +
					  "`region` varchar(250) DEFAULT NULL," +
					  "`population` int(12) DEFAULT NULL," +
					  "`lat` float(10,6) DEFAULT NULL," +
					  "`lon` float(10,6) DEFAULT NULL" +
					  ")");
				
				db.tryStatement(localConn, "CREATE INDEX city_idx ON world_cities (city)");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void readCSV(String filename) { 
		BufferedReader br = null;
		try {
			InputStream is = new FileInputStream(filename);
			br = new BufferedReader(new InputStreamReader(is));
			
			String lineString = br.readLine();
			List<String> line = CsvParser.fsmParse(lineString);
			columns = new ArrayList<String>(line);
			rows = new ArrayList<DataRow>();
			while ((lineString = br.readLine()) != null) {
				line = CsvParser.fsmParse(lineString);
				if (line.size()==columns.size()) {
					DataRow row = new DataRow(columns, line);					
					rows.add(row);
				} else {
					System.out.println(line.size() + ": " + lineString);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br!=null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static Set<String> getUniquePlaces(Connection localConn) {
		Set<String> result = new HashSet<String>();
		for (int i = 0; i < 533661; i+= 50) {
			if (i%1000 == 0) System.out.println("Getting places: " + i + " of " + 533661);
			String sqlStr = "SELECT distinct location FROM backpage_ads where id between " + i + " AND " + (i+50)+ ";";
			Statement stmt = null;
			try {
				stmt = localConn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					result.add(rs.getString("location"));
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
		return result;
	}
	
	
	@SuppressWarnings("unused")
	private static Set<String> getUngeocodablePlaces(TableDB db, Connection localConn, Set<String> places) {
		Set<String> result = new HashSet<String>();
		
		int count= 0;
		for (String place : places) {
			System.out.println("Geocoding: " + count + " of " + places.size());
			if (result.contains(place)) {
				continue;
			}
			Pair<Float,Float> latlon = Geocoder.geocodeAlphaOnly(db, localConn, place);
			if (latlon == null || latlon.getFirst() == null || latlon.getSecond() == null) {
				result.add(place);
			}
			count++;
		}
		
		return result;
	}

	
	public static void initializeGeocoderDatabase() {
		TableDB db = TableDB.getInstance(localdb_name, localdb_type, localdb_hostname, localdb_port,localdb_user, localdb_password, "");
		Connection localConn = db.open();
		
		if (!db.tableExists(CITIES_TABLE_NAME)) {
			readCSV(worldCitiesTextFile);
			createWorldCitiesTable(db, localConn);
			int count = 0;
			for (DataRow row : rows) {
				if ((++count)%1000==0) System.out.println(count + " of " + rows.size());
				insertCity(localConn, row);
			}
		}
		
		if (!db.tableExists(STATES_TABLE_NAME)) {	
			readCSV(stateAndProvincesTextFile);
			createStatesAndProvincesTable(db, localConn);
			for (DataRow row : rows) {
				insertState(localConn, row);
			}
		}
		
		try {
			Set<String> countries = Geocoder.getUniqueCountries(localConn);
			Set<String> uniquePlaces = Geocoder.getUniquePlaces(localConn);
			//Set<String> uniqueRegions = Geocoder.getUniqueRegions(localConn);
			//Set<String> ungeocodablePlaces = Geocoder.getUngeocodablePlaces(db, localConn, uniquePlaces);
			writeSet("uniqueCountries.txt",countries);
			writeSet("uniquePlaces.txt",uniquePlaces);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
	}

	@SuppressWarnings("unused")
	private static Set<String> getUniqueRegions(Connection localConn) {
		Set<String> result = new HashSet<String>();
		for (int i = 0; i < 533661; i+= 50) {
			if (i%1000 == 0) System.out.println("Getting places: " + i + " of " + 533661);
			String sqlStr = "SELECT distinct region FROM ads where id between " + i + " AND " + (i+50)+ ";";
			Statement stmt = null;
			try {
				stmt = localConn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					result.add(rs.getString("region"));
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
		return result;
	}

	private static void writeSet(String fileName, Set<String> set) {
		try {
			BufferedWriter out = new BufferedWriter( new FileWriter(fileName) );
			for (String s : set) {
				out.write(s + "\n");
			}
			out.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static Set<String> getUniqueCountries(Connection localConn) {
		Set<String> result = new HashSet<String>();
		for (int i = 0; i < 465449; i+= 50) {
			if (i%1000 == 0) System.out.println("Getting countries: " + i + " of " + 465449);
			String sqlStr = "SELECT distinct location FROM ads where id between " + i + " AND " + (i+50)+ ";";
			Statement stmt = null;
			try {
				stmt = localConn.createStatement();
				ResultSet rs = stmt.executeQuery(sqlStr);
				while (rs.next()) {
					String locString = rs.getString("location");
					if (locString != null) {
						locString = locString.toLowerCase().replace(" ", "");
						String []pieces = locString.split(",");
						result.add(pieces[pieces.length-1]);
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
		return result;
	}
	
	public static void addValues(TableDB db, Connection conn) {
		String sqlStr = "insert into world_cities (country, city, accentcity,region,population,lat,lon,cityalpha) " +
				"values (\"us\",\"queens\",\"Queens\",\"NY\",2273000,40.7500,-73.8667,\"queens\")," +
				"(\"us\",\"manhattan\",\"Manhattan\",\"NY\",1619000,40.7903,-73.9597,\"manhattan\")";
		db.tryStatement(conn, sqlStr);
	}
	
	public static void main(String[] args) {
		TableDB db = TableDB.getInstance(localdb_name, localdb_type, localdb_hostname, localdb_port,localdb_user, localdb_password, "");
		Connection localConn = db.open();
//		Pair<Float,Float> location = Geocoder.geocodeCity(db, localConn, "toronto", true);
//		Pair<Float,Float> location = Geocoder.geocode(db, localConn, "Washington, USA");
//		addValues(db, localConn);
		Pair<Float,Float> location = Geocoder.geocode(db, localConn, "Twin Cities, MN, USA");
		System.out.println(location.getFirst() + "," + location.getSecond());
		db.close();
	}
	
}

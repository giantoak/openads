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
package oculus.memex.rest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import oculus.memex.clustering.Cluster;
import oculus.memex.clustering.ClusterDetails;
import oculus.memex.concepts.AdKeywords;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.geo.AdLocations;
import oculus.memex.image.ImageHistogramHash;
import oculus.memex.init.PropertyManager;
import oculus.memex.tags.Tags;
import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.ClustersDetailsResult;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.StringUtil;
import oculus.xdataht.util.TimeLog;

@Path("/preclusterDetails")
public class PreclusterDetailsResource  {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("{preclusterType}/{clusterId}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusterDetailsResult handleGet(@PathParam("preclusterType") String preclusterType, @PathParam("clusterId") Integer clusterId, @Context HttpServletRequest request) {
		List<DataRow> results = new ArrayList<DataRow>();
		TimeLog log = new TimeLog();
		log.pushTime("Precluster details: " + preclusterType + ":" + clusterId);
		log.pushTime("Fetch Ad IDs");
		HashSet<Integer> members = new HashSet<Integer>();
		Cluster.getAdsInCluster(clusterId, members, 2000);
		log.popTime();
		log.pushTime("Fetch Ad Contents");
		getDetails(members, results, request.getRemoteUser());
		log.popTime();

		log.pushTime("Prepare results");

		ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(results);

		ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
		for (HashMap<String,String> map : details) {
			serializableDetails.add( new StringMap(map));
		}
		log.popTime();
		log.popTime();
		return new ClusterDetailsResult(serializableDetails);		
	}
	@POST
	@Path("fetchAds")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClustersDetailsResult fetchAds(String clustersIds, @Context HttpServletRequest request) {
		TimeLog log = new TimeLog();
		log.pushTime("Cluster tip search");
		HashMap<Integer, ClusterDetailsResult> results = new HashMap<Integer, ClusterDetailsResult>();
		try {
			JSONObject jo = new JSONObject(clustersIds);
			JSONArray clusterids = jo.getJSONArray("ids");
			HashMap<Integer,HashSet<Integer>> clusterAds = new HashMap<Integer,HashSet<Integer>>();

			log.pushTime("Fetch Ad IDs");
			Cluster.getAdsInClusters(clusterids, clusterAds, 2000);
			log.popTime();
			
			log.pushTime("Fetch Ad Contents");	
			for(Integer clusterid: clusterAds.keySet()) {
				log.pushTime("Fetch contents for cluster: " + clusterid);
				HashSet<Integer> members = clusterAds.get(clusterid);
				ArrayList<DataRow> result = new ArrayList<DataRow>();
				PreclusterDetailsResource.getDetails(members, result, request.getRemoteUser());
				log.popTime();

				log.pushTime("Prepare results for cluster: " + clusterid);
				ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(result);
				ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
				for (HashMap<String,String> map : details) {
					serializableDetails.add( new StringMap(map));
				}
				results.put(clusterid, new ClusterDetailsResult(serializableDetails));
				log.popTime();
			}
			log.popTime();
			log.popTime();
			return new ClustersDetailsResult(results);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static void getDetails(HashSet<Integer> ads, List<DataRow> results, String user_name) {
		StringBuffer adstring = new StringBuffer("(");
		boolean isFirst = true;
		for (Integer ads_id:ads) {
			if (isFirst) isFirst = false;
			else adstring.append(",");
			adstring.append(ads_id);
		}
		adstring.append(")");

		HashMap<Integer,DataRow> adDetails = new HashMap<Integer,DataRow>();

		HashMap<Integer, HashSet<Pair<String,String>>> adKeywords = AdKeywords.getAdKeywords(adstring.toString());
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		HashMap<Integer,String> sources = ClusterDetails.getSources(htconn);
		htdb.close();
		
		getMainDetails(adKeywords, adDetails, sources, adstring);
		getExtraDetails(adDetails, adstring);
		getLocations(adDetails, adstring);
		getTags(adDetails, adstring, user_name);
		getImages(adDetails, adstring);
		
		for (DataRow row:adDetails.values()) {
			results.add(row);
		}
	}

	private static void getImages(HashMap<Integer, DataRow> adDetails, StringBuffer adstring) {
		HashMap<Integer,HashSet<Integer>> imageFeatureMap = new HashMap<Integer,HashSet<Integer>>();
		MemexHTDB db = MemexHTDB.getInstance();
		Connection conn = db.open();
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ht.id, o.ads_id, ht.location"
						+ " FROM " + ScriptDBInit._htSchema + ".images AS ht JOIN"
						+ " (SELECT images_id, ads_id FROM " + ScriptDBInit._oculusSchema + " .images"    
						+ " WHERE ads_id IN " + adstring.toString() + " ) AS o ON ht.id = o.images_id" );
			int ads_id, images_id;
			HashSet<Integer> imageFeatures;
			HashMap<Integer,HashSet<Integer>> adsToImages = new HashMap<Integer,HashSet<Integer>>();
			HashMap<Integer,String> newImages = new HashMap<Integer,String>();
			while(rs.next()) {			
				ads_id = rs.getInt("ads_id");
				images_id = rs.getInt("id");
				HashSet<Integer> imageId = adsToImages.get(ads_id);
				if(imageId == null) {
					imageId = new HashSet<Integer>();
					adsToImages.put(ads_id, imageId);
				}
				if(!imageId.contains(images_id)) {
					imageId.add(images_id);
					String imageUrl = rs.getString("location");
					adAttribute(ads_id, "images", imageUrl, adDetails);
					adAttribute(ads_id, "images_id", Integer.toString(images_id), adDetails);
					newImages.put(images_id, imageUrl);
				}
				imageFeatures = imageFeatureMap.get(ads_id);
				if(imageFeatures==null) {
					imageFeatures = new HashSet<Integer>();
					imageFeatures.add(images_id);
					imageFeatureMap.put(ads_id, imageFeatures);
				} else {
					imageFeatures.add(rs.getInt("id"));
				}
			}
			String hashImages = PropertyManager.getInstance().getProperty(PropertyManager.HASH_IMAGES);
			if (hashImages!=null && hashImages.compareTo("true")==0) ImageHistogramHash.addImages(newImages);
			for (Map.Entry<Integer, DataRow> adentry:adDetails.entrySet()) {
				int adid = adentry.getKey();
				DataRow ad = adentry.getValue();
				String idStr = ad.get("images_id");
				if (idStr==null) continue;
				String[] ids = idStr.split(",");
				for (String imagesidStr:ids) {
					int imageid = Integer.parseInt(imagesidStr);
					adAttribute(adid, "images_hash", ImageHistogramHash.getHash(imageid), adDetails);
				}
			}
				
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.close();
		getImageFeatures(adDetails, imageFeatureMap);
	}

	private static void getImageFeatures(HashMap<Integer, DataRow> adDetails, HashMap<Integer,HashSet<Integer>> imageFeatureMap) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		for(Integer ads_id:imageFeatureMap.keySet()) {
			try {
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT images_id, value "
						+ "FROM images_attributes "
						+ "WHERE images_id IN " + StringUtil.hashSetToSqlList(imageFeatureMap.get(ads_id)));
				while(rs.next()) {
					adAttribute(ads_id, "imageFeatures", rs.getString("value"), adDetails);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		db.close();		
	}

	private static void getTags(HashMap<Integer, DataRow> adDetails,
			StringBuffer adstring, String user_name) {
		MemexOculusDB oculusDB = MemexOculusDB.getInstance();
		Connection oculusConn = oculusDB.open();
		try {
			Statement stmt = oculusConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ad_id,tag FROM " +
					Tags.TAGS_TABLE + " WHERE user_name='" + user_name + "' AND ad_id IN" + adstring.toString());
			while(rs.next()) {				
				adAttribute(rs.getInt("ad_id"), "tags", rs.getString("tag"), adDetails);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		oculusDB.close();
	}

	private static void getMainDetails(
			HashMap<Integer, HashSet<Pair<String, String>>> adKeywords,
			HashMap<Integer, DataRow> result,
			HashMap<Integer,String> sources,
			StringBuffer adstring) {
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT id,phone,email,website,sources_id,title,text,url,posttime,first_id FROM ads WHERE id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("id");
				adAttribute(adid, "mainphone", rs.getString("phone"), result);
				adAttribute(adid, "email", rs.getString("email"), result);
				adAttribute(adid, "websites", rs.getString("website"), result);
				adAttribute(adid, "source", sources.get(rs.getInt("sources_id")), result);
				adAttribute(adid, "title", rs.getString("title"), result);
				adAttribute(adid, "text", rs.getString("text"), result);
				adAttribute(adid, "url", rs.getString("url"), result);
				Timestamp timestamp = rs.getTimestamp("posttime");
				adAttribute(adid, "posttime", ""+(timestamp==null?0:timestamp.getTime()), result);
				adAttribute(adid, "first_id", rs.getString("first_id"), result);
			}
		} catch (Exception e) {
			System.out.println("Failed: " + sqlStr);
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		htdb.close();
	}

	private static void adAttribute(int adid, String attribute, String value, HashMap<Integer, DataRow> result) {
		DataRow dataRow = result.get(adid);
		if (dataRow==null) {
			dataRow = new DataRow();
			dataRow.put("id", ""+adid);
			result.put(adid, dataRow);
		}
		String oldValue = dataRow.get(attribute);
		if (oldValue==null) {
			dataRow.put(attribute, value);
		} else {
			dataRow.put(attribute, oldValue + "," + value);
		}
	}

	private static void setAttribute(int adid, String attribute, String value, HashMap<Integer, DataRow> result) {
		DataRow dataRow = result.get(adid);
		if (dataRow==null) {
			dataRow = new DataRow();
			dataRow.put("id", ""+adid);
			result.put(adid, dataRow);
		}
		dataRow.put(attribute, value);
	}

	private static void getExtraDetails(HashMap<Integer, DataRow> result, StringBuffer adstring) {
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT ads_id,attribute,value FROM ads_attributes where ads_id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = htconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				String attribute = rs.getString("attribute");
				String value = rs.getString("value");
				adAttribute(adid, attribute, value, result);
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
		htdb.close();
	}
	
	private static void getLocations(HashMap<Integer, DataRow> result, StringBuffer adstring) {
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		String sqlStr;
		Statement stmt;
		sqlStr = "SELECT ads_id,label,latitude,longitude FROM " + AdLocations.AD_LOCATIONS_TABLE + " WHERE ads_id IN " + adstring.toString();
		stmt = null;
		try {
			stmt = oculusconn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				int adid = rs.getInt("ads_id");
				String location = rs.getString("label");
				Float latitude = rs.getFloat("latitude");
				Float longitude = rs.getFloat("longitude");
				setAttribute(adid, "latitude", ""+latitude, result);
				setAttribute(adid, "longitude", ""+longitude, result);
				setAttribute(adid, "location", location, result);
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
		oculusdb.close();
	}
	
}

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

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

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
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.memex.aggregation.AttributeLocation;
import oculus.memex.aggregation.LocationCluster;
import oculus.memex.aggregation.LocationTimeAggregation;
import oculus.memex.aggregation.SourceAggregation;
import oculus.memex.aggregation.TimeAggregation;
import oculus.memex.clustering.AttributeDetails;
import oculus.memex.clustering.Cluster;
import oculus.memex.clustering.ClusterDetails;
import oculus.memex.clustering.MemexAd;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.geo.Demographics;
import oculus.memex.graph.AttributeLinks;
import oculus.xdataht.model.DemographicResult;
import oculus.xdataht.model.DemographicResults;
import oculus.xdataht.model.LocationTimeVolumeResult;
import oculus.xdataht.model.LocationTimeVolumeResults;
import oculus.xdataht.model.TimeVolumeResult;
import oculus.xdataht.model.TimeVolumeResults;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.TimeLog;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;


@Path("/overview")
public class OverviewResource {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("timeseries")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public TimeVolumeResults getTimeSeries() {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch global time series");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<TimeVolumeResult> a = new ArrayList<TimeVolumeResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + TimeAggregation.TIME_TABLE);  
			while (rs.next()) {
				a.add(new TimeVolumeResult(rs.getLong("day")*1000, rs.getInt("count")));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		Collections.sort(a, new Comparator<TimeVolumeResult>() {
			public int compare(TimeVolumeResult o1, TimeVolumeResult o2) {
				return (int)((o1.getDay()-o2.getDay())/1000);
			};
		});
		TimeVolumeResults results = new TimeVolumeResults(a);
		log.popTime();
		return results;
	}
	
	@GET
	@Path("sourcecounts")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getSourceCounts() {
		ArrayList<Pair<Integer,String>> counts = new ArrayList<Pair<Integer,String>>();
		TimeLog log = new TimeLog();
		log.pushTime("Fetch global source counts");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + SourceAggregation.SOURCE_TABLE);  
			while (rs.next()) {
				counts.add(new Pair<Integer,String>(rs.getInt("count"), rs.getString("source")));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		Collections.sort(counts, new Comparator<Pair<Integer,String>>() {
			public int compare(Pair<Integer,String> o1, Pair<Integer,String> o2) {
				return (o2.getFirst()-o1.getFirst());
			};
		});
		log.popTime();
		
		JSONObject result = new JSONObject();
		
		JSONArray jarray = new JSONArray();
		try {
			result.put("sourcecounts", jarray);
			for (Pair<Integer,String> count:counts) {
				JSONObject sourcecount = new JSONObject();
				sourcecount.put("source", count.getSecond());
				sourcecount.put("count", count.getFirst());
				jarray.put(sourcecount);
			}		
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	@GET
	@Path("locationtime")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public LocationTimeVolumeResults getLocationTimes() {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch all location time series");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<LocationTimeVolumeResult> a = new ArrayList<LocationTimeVolumeResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + LocationTimeAggregation.LOCATION_TIME_TABLE);  
			String currentLocation = "unset";
			LocationTimeVolumeResult currentResult = null;
			ArrayList<TimeVolumeResult> currentTimeSeries = null;
			while (rs.next()) {
				String location = rs.getString("location");
				Float lat = rs.getFloat("lat");
				Float lon = rs.getFloat("lon");
				if (!currentLocation.equals(location)) {
					currentTimeSeries = new ArrayList<TimeVolumeResult>();
					currentResult = new LocationTimeVolumeResult(location, lat, lon, currentTimeSeries);
					currentLocation = location;
					a.add(currentResult);
				}
				currentTimeSeries.add(new TimeVolumeResult(rs.getLong("day")*1000, rs.getInt("count")));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		LocationTimeVolumeResults results = new LocationTimeVolumeResults(a);
		log.popTime();
		return results;
	}
	
	@POST
	@Path("locationtimeseries")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getLocationTimeseries(String location) {
		TimeLog log = new TimeLog();
		log.pushTime("Location Timeseries: " + location);

		String sqlStr = "SELECT date, NewToTown, count, baseline, expected, p_value from memex_cmu.timeseries" +
				" WHERE location='" + location + "' ORDER BY date DESC";
		HashMap<Long,Float[]> timeseries = new HashMap<Long,Float[]>();
		Calendar c = Calendar.getInstance();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);  
			while (rs.next()) {
				int newToTown = rs.getInt("NewToTown")+1;
				Float[] rowvals = new Float[]{rs.getFloat("count"),rs.getFloat("baseline"),rs.getFloat("expected"),rs.getFloat("p_value")};
				Timestamp timestamp = rs.getTimestamp("date");
				long time = (timestamp==null)?0:timestamp.getTime();
				if (time<=0) continue;
				c.setTimeInMillis(time);
				c.set(Calendar.HOUR,0);
				c.set(Calendar.MINUTE,0);
				c.set(Calendar.SECOND,0);
				c.set(Calendar.MILLISECOND,0);
				time = c.getTimeInMillis()/1000;
				
				Float[] vals = timeseries.get(time);
				if (vals==null) {
					vals = new Float[]{0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f,0f};
					timeseries.put(time, vals);
				}
				for (int i=0; i<4; i++) {
					Float rowval = rowvals[i];
					if (rowval!=null) vals[newToTown*4+i] = rowval;
				}
			}
		} catch (Exception e) {
			System.out.println("**WARNING** Failed to load location timeseries: " + e.getMessage());
		}
		db.close();

		ArrayList<Pair<Long,Float[]>> orderedResult = new ArrayList<Pair<Long,Float[]>>();
		for (Long time:timeseries.keySet()) {
			orderedResult.add(new Pair<Long,Float[]>(time,timeseries.get(time)));
		}
		Collections.sort(orderedResult, new Comparator<Pair<Long,Float[]>>() {
			@Override
			public int compare(Pair<Long, Float[]> o1, Pair<Long, Float[]> o2) {
				return (o1.getFirst()-o2.getFirst()<0)?-1:1;
			}			
		});
		
		log.popTime();
		
		JSONObject result = new JSONObject();
		try {
			JSONArray jFeatures = new JSONArray();
			result.put("features", jFeatures);
			for (int i=0; i<16; i++) {
				JSONObject jLine = new JSONObject();
				jFeatures.put(jLine);
				String type = "count";
				if (i%4==1) type = "baseline";
				if (i%4==2) type = "expected";
				if (i%4==3) type = "p-value";
				jLine.put("type", type);
				String newToTown = "Total";
				if ((int)(i/4)==1) newToTown = "Local";
				if ((int)(i/4)==2) newToTown = "New to town";
				if ((int)(i/4)==3) newToTown = "New to ads";
				jLine.put("newToTown", newToTown);
				JSONArray jTimeseries = new JSONArray();
				jLine.put("data",jTimeseries);
				for (Pair<Long,Float[]> day:orderedResult) {
					JSONArray jDay = new JSONArray();
					jDay.put(day.getFirst());
					Float[] dayData = day.getSecond();
					jDay.put(dayData[i]);
					jTimeseries.put(jDay);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return result.toString();
	}
	
	@GET
	@Path("demographics/{column}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public DemographicResults getDemographics(@PathParam("column") String column) {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch demographics");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<DemographicResult> a = new ArrayList<DemographicResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT location,latitude,longitude," + column + " FROM " + Demographics.DEMOGRAPHICS_TABLE +
					" inner join locations on locations.label=demographics.location");  
			while (rs.next()) {
				String location = rs.getString("location");
				Float lat = rs.getFloat("latitude");
				Float lon = rs.getFloat("longitude");
				Float value = rs.getFloat(column);
				a.add(new DemographicResult(location, lat, lon, value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		DemographicResults results = new DemographicResults(a);
		log.popTime();
		return results;
	}
	
	@GET
	@Path("ldemographics")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public DemographicResults getLocationDemographics() {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch demographics");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<DemographicResult> a = new ArrayList<DemographicResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT location,latitude,longitude,rape,robbery,expenditures,ads,white,black FROM " + Demographics.DEMOGRAPHICS_TABLE +
					" inner join locations on locations.label=location_demographics.location");  
			while (rs.next()) {
				String location = rs.getString("location");
				Float lat = rs.getFloat("latitude");
				Float lon = rs.getFloat("longitude");
				Float rape = rs.getFloat("rape");
				Float robbery = rs.getFloat("robbery");
				Float expenditures = rs.getFloat("expenditures");
				Float ads = rs.getFloat("ads");
				Float white = rs.getFloat("white");
				Float black = rs.getFloat("black");
				a.add(new DemographicResult(location, lat, lon, rape, robbery, expenditures, ads, white, black));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		DemographicResults results = new DemographicResults(a);
		log.popTime();
		return results;
	}
	
	@POST
	@Path("locationclusterdetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getLocationClusterDetails(String location) {
		TimeLog log = new TimeLog();
		log.pushTime("Location Cluster Details: " + location);
		log.pushTime("Scores");
		String sqlScoreStr = "SELECT target_id,score FROM " + LocationCluster.LOCATION_CLUSTER_TABLE + " INNER JOIN memex_hti.htic_clusters_details ON htic_clusters_details.target_id=" +
				LocationCluster.LOCATION_CLUSTER_TABLE + ".clusterid WHERE location='" + location + "'";
		HashMap<String, Float> scores = getClusterScores(sqlScoreStr);
		log.popTime();
		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + LocationCluster.LOCATION_CLUSTER_TABLE + ".clusterid as id,matches," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,clustername as name,latestad FROM " + 
				LocationCluster.LOCATION_CLUSTER_TABLE + " INNER JOIN " + ClusterDetails.CLUSTER_DETAILS_TABLE +
				" ON " + LocationCluster.LOCATION_CLUSTER_TABLE + ".clusterid=" + ClusterDetails.CLUSTER_DETAILS_TABLE + ".clusterid" +
				" WHERE location='" + location + "' ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, null, false, scores);
		result.append("]}");
		log.popTime();
		return result.toString();
	}

	private HashMap<String,Float> getClusterScores(String sqlStr) {
		HashMap<String,Float> result = new HashMap<String,Float>();
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String clusterid = rs.getString("target_id");
				Float score = rs.getFloat("score");
				result.put(clusterid, score);
			}
		} catch (Exception e) {
			System.out.println("**WARNING** Failed to load HTI scores: " + e.getMessage());
		}
		db.close();
		return result;
	}
	
	@POST
	@Path("locationattributedetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getLocationAttributeDetails(String location) {
		TimeLog log = new TimeLog();
		log.pushTime("Location Attribute Details: " + location);
		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + AttributeLocation.ATTRIBUTE_LOCATION_TABLE + ".attributeid as id,matches," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,value as name,latestad FROM " + 
				AttributeLocation.ATTRIBUTE_LOCATION_TABLE + " INNER JOIN " + AttributeDetails.ATTRIBUTE_DETAILS_TABLE +
				" ON " + AttributeLocation.ATTRIBUTE_LOCATION_TABLE + ".attributeid=" + AttributeDetails.ATTRIBUTE_DETAILS_TABLE + ".id" +
				" WHERE location='" + location + "' ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, null, false);
		result.append("]}");
		log.popTime();
		return result.toString();
	}

	@POST
	@Path("tipclusterdetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getTipClusterDetails(String tip) {
		TimeLog log = new TimeLog();
		log.pushTime("Tip Cluster Details: " + tip);

		if (tip==null || tip.trim().length()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}
		
		HashSet<Integer> matchingAds = GraphResource.fetchMatchingAds(tip, log);
		if (matchingAds==null||matchingAds.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		log.pushTime(" Get clusters for search results");
		HashMap<String,Integer> matchingClusters = Cluster.getSimpleClusterCounts(matchingAds);
		log.popTime();
		if (matchingClusters==null||matchingClusters.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		String clusteridList = "(";
		boolean isFirst = true;
		for (String clusterid:matchingClusters.keySet()) {
			if (isFirst) isFirst = false;
			else clusteridList += ",";
			clusteridList += clusterid;
		}
		clusteridList += ")";

		log.pushTime("Scores");
		String sqlScoreStr = "SELECT target_id,score FROM memex_hti.htic_clusters_details WHERE target_id IN " + clusteridList;
		HashMap<String, Float> scores = getClusterScores(sqlScoreStr);
		log.popTime();

		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + ClusterDetails.CLUSTER_DETAILS_TABLE + ".clusterid as id," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,clustername as name,latestad FROM " + 
				ClusterDetails.CLUSTER_DETAILS_TABLE +
				" WHERE clusterid IN " + clusteridList + " ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, matchingClusters, false, scores);
		result.append("]}");
		log.popTime();
		return result.toString();
	}

	@POST
	@Path("tipattributedetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getTipAttributeDetails(String tip) {
		TimeLog log = new TimeLog();
		log.pushTime("Tip Attribute Details: " + tip);

		if (tip==null || tip.trim().length()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}
		
		HashSet<Integer> matchingAds = GraphResource.fetchMatchingAds(tip, log);
		if (matchingAds==null||matchingAds.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		log.pushTime(" Get attributes for search results");

		HashSet<String> matchingAttributeValues = new HashSet<String>();
		HashMap<String,Integer> matchingAttributes = new HashMap<String,Integer>();
		MemexHTDB htdb = MemexHTDB.getInstance();
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection htconn = htdb.open();
		Connection oculusconn = oculusdb.open();
		try {
			HashMap<Integer, MemexAd> adbatch = MemexAd.fetchAdsOculus(htconn, oculusconn, matchingAds);
			for (MemexAd ad:adbatch.values()) {
				for (Entry<String,HashSet<String>> e:ad.attributes.entrySet()) {
					// String attribute = e.getKey();
					for (String value:e.getValue()) {
						String lcval = value.toLowerCase();
						matchingAttributeValues.add(lcval);
						Integer matchCount = matchingAttributes.get(lcval);
						if (matchCount==null) matchCount = 0;
						matchCount++;
						matchingAttributes.put(lcval, matchCount);
						
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		htdb.close();
		oculusdb.close();

		log.popTime();
		
		StringBuilder valuesStr = new StringBuilder();
		boolean isFirst = true;
		for (String val:matchingAttributeValues) {
			if (isFirst) isFirst = false;
			else valuesStr.append(",");
			valuesStr.append("'" + val + "'");
		}
		
		log.pushTime("Get details for attributes");
		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + AttributeLinks.ATTRIBUTES_TABLE + ".id as id," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,latestad," + 
				AttributeDetails.ATTRIBUTE_DETAILS_TABLE + ".value as name FROM " + 
				AttributeLinks.ATTRIBUTES_TABLE + " INNER JOIN " + AttributeDetails.ATTRIBUTE_DETAILS_TABLE +
				" ON " + AttributeLinks.ATTRIBUTES_TABLE + ".id=" + AttributeDetails.ATTRIBUTE_DETAILS_TABLE + ".id" +
				" WHERE " + AttributeLinks.ATTRIBUTES_TABLE + ".value IN (" + valuesStr.toString() + ") ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, matchingAttributes, true);
		result.append("]}");
		log.popTime();
		log.popTime();
		return result.toString();
	}

	@POST
	@Path("csvclusterdetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getCSVClusterDetails(String phoneStr) {
		TimeLog log = new TimeLog();
		String[] phones = phoneStr.split(",");
		log.pushTime("CSV Cluster Details: " + phones.length);

		HashSet<Integer> matchingAds = GraphResource.fetchMatchingAds(phones, log);
		if (matchingAds==null||matchingAds.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		log.pushTime(" Get clusters for search results");
		HashMap<String,Integer> matchingClusters = Cluster.getSimpleClusterCounts(matchingAds);
		log.popTime();
		if (matchingClusters==null||matchingClusters.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		String clusteridList = "(";
		boolean isFirst = true;
		for (String clusterid:matchingClusters.keySet()) {
			if (isFirst) isFirst = false;
			else clusteridList += ",";
			clusteridList += clusterid;
		}
		clusteridList += ")";

		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + ClusterDetails.CLUSTER_DETAILS_TABLE + ".clusterid as id," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,clustername as name,latestad FROM " + 
				ClusterDetails.CLUSTER_DETAILS_TABLE +
				" WHERE clusterid IN " + clusteridList + " ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, matchingClusters, false);
		result.append("]}");
		log.popTime();
		return result.toString();
	}
	private void fetchClusterDetails(String sqlStr, TimeLog log, StringBuilder result, HashMap<String,Integer> matchingClusters, boolean nameIsId) {
		fetchClusterDetails(sqlStr, log, result, matchingClusters, nameIsId, null);
	}
	private void fetchClusterDetails(String sqlStr, TimeLog log, StringBuilder result, HashMap<String,Integer> matchingClusters, boolean nameIsId, HashMap<String,Float> scores) {
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		try {
			log.pushTime("SQL Select");
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			log.popTime();
			log.pushTime("Build result");
			boolean isFirst = true;
			while (rs.next()) {
				if (isFirst) {
					isFirst = false;
				} else {
					result.append(",");
				}
				String clusterid = rs.getString("id");
				int adcount = rs.getInt("adcount");
				String phonelist = rs.getString("phonelist");
				String emaillist = rs.getString("emaillist");
				String weblist = rs.getString("weblist");
				String namelist = rs.getString("namelist");
				String ethnicitylist = rs.getString("ethnicitylist");
				String timeseries = rs.getString("timeseries");
				String locationlist = rs.getString("locationlist");
				String sourcelist = rs.getString("sourcelist");
				String keywordlist = rs.getString("keywordlist");
				String clustername = rs.getString("name");
				String latestad = rs.getString("latestad");
				if (phonelist==null) phonelist = "";
				if (emaillist==null) emaillist = "";
				if (weblist==null) weblist = "";
				if (namelist==null) namelist = "";
				if (ethnicitylist==null) ethnicitylist = "";
				if (timeseries==null) timeseries = "";
				if (locationlist==null) locationlist = "";
				if (sourcelist==null) sourcelist = "";
				if (keywordlist==null) keywordlist = "";
				if (clustername==null) clustername = "";
				if (latestad==null) latestad = "";
				if (latestad.length()>10)
					latestad=latestad.substring(0,10);
				result.append("{\"id\":");
				result.append(clusterid); 
				result.append(",\"ads\":");
				result.append(adcount);
				if (clustername!=null) {
					clustername = Jsoup.clean(clustername, Whitelist.none());
					result.append(",\"clustername\":\"");
					result.append(clustername);
					result.append("\"");
				}
				if (scores!=null) {
					Float score = scores.get(clusterid);
					if (score!=null) score = Math.round(score*100)/100f;
					result.append(",\"score\":");
					result.append(score);
				}
				if (matchingClusters!=null) {
					result.append(",\"matches\":");
					result.append(matchingClusters.get(nameIsId?clustername:clusterid));
				} else {
					result.append(",\"matches\":");
					result.append(rs.getInt("matches"));
				}
				result.append(",\"phonelist\":{");
				result.append(phonelist);
				result.append("},\"emaillist\":{");
				result.append(emaillist);
				result.append("},\"weblist\":{");
				result.append(weblist);
				result.append("},\"namelist\":{");
				result.append(namelist);
				result.append("},\"ethnicitylist\":{");
				result.append(ethnicitylist);
				result.append("},\"timeseries\":{");
				result.append(timeseries);
				result.append("},\"locationlist\":{");
				result.append(locationlist);
				result.append("},\"sourcelist\":{");
				result.append(sourcelist);
				result.append("},\"keywordlist\":{");
				result.append(keywordlist);
				result.append("},\"latestad\":\"");
				result.append(latestad);
				result.append("\"}");
			}
			log.popTime();

		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
	}
	
}

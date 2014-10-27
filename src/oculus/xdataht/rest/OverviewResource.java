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
package oculus.xdataht.rest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.attributes.AttributeDetails;
import oculus.xdataht.attributes.Attributes;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.LocationTimeVolumeResult;
import oculus.xdataht.model.LocationTimeVolumeResults;
import oculus.xdataht.model.LocationVolumeResult;
import oculus.xdataht.model.LocationVolumeResults;
import oculus.xdataht.model.TimeVolumeResult;
import oculus.xdataht.model.TimeVolumeResults;
import oculus.xdataht.util.TimeLog;


@Path("/overview")
public class OverviewResource {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("locations")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public LocationVolumeResults getLocations() {
		TimeLog log = new TimeLog();
		log.pushTime("Locations");
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<LocationVolumeResult> a = new ArrayList<LocationVolumeResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + TableDB.LOCATION_TABLE);  
			while (rs.next()) {
				a.add(new LocationVolumeResult(rs.getInt("count"), 
						rs.getFloat("lat"), 
						rs.getFloat("lon"), 
						rs.getString("location")));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		Collections.sort(a, new Comparator<LocationVolumeResult>() {
			public int compare(LocationVolumeResult o1, LocationVolumeResult o2) {
				return o2.getCount()-o1.getCount();
			};
		});
		LocationVolumeResults results = new LocationVolumeResults(a);
		log.popTime();
		return results;
	}
	
	@GET
	@Path("timeseries")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public TimeVolumeResults getTimeSeries() {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch global time series");
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<TimeVolumeResult> a = new ArrayList<TimeVolumeResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + TableDB.TIME_TABLE);  
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
	@Path("locationtime")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public LocationTimeVolumeResults getLocationTimes() {
		TimeLog log = new TimeLog();
		log.pushTime("Fetch all location time series");
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		ArrayList<LocationTimeVolumeResult> a = new ArrayList<LocationTimeVolumeResult>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + TableDB.LOCATION_TIME_TABLE);  
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
	@Path("locationclusterdetails")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getLocationClusterDetails(String location) {
		TimeLog log = new TimeLog();
		log.pushTime("Location Cluster Details: " + location);
		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + TableDB.LOCATION_CLUSTER_TABLE + ".clusterid," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				TableDB.LOCATION_CLUSTER_TABLE + " INNER JOIN " + TableDB.CLUSTER_DETAILS_TABLE +
				" ON " + TableDB.LOCATION_CLUSTER_TABLE + ".clusterid=" + TableDB.CLUSTER_DETAILS_TABLE + ".clusterid" +
				" WHERE location='" + location + "' ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, null);
		result.append("]}");
		log.popTime();
		return result.toString();
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
		String sqlStr = "SELECT " + AttributeDetails.ATTRIBUTES_LOCATIONS_TABLE + ".attributes_id as clusterid," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				AttributeDetails.ATTRIBUTES_LOCATIONS_TABLE + " INNER JOIN " + AttributeDetails.ATTRIBUTES_DETAILS_TABLE +
				" ON " + AttributeDetails.ATTRIBUTES_LOCATIONS_TABLE + ".attributes_id=" + AttributeDetails.ATTRIBUTES_DETAILS_TABLE + ".attributes_id" +
				" WHERE location='" + location + "' ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, null);
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
		
		ArrayList<String> matchingAds = GraphResource.fetchMatchingAds(tip, log);
		if (matchingAds==null||matchingAds.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		log.pushTime(" Get attributes for search results");
		HashMap<String,Integer> matchingAttributes = Attributes.getAdCounts(matchingAds);
		log.popTime();
		if (matchingAttributes==null||matchingAttributes.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		String attributeidList = "(";
		boolean isFirst = true;
		for (String attributeid:matchingAttributes.keySet()) {
			if (isFirst) isFirst = false;
			else attributeidList += ",";
			attributeidList += attributeid;
		}
		attributeidList += ")";

		StringBuilder result = new StringBuilder(200);
		result.append("{\"details\":[");
		String sqlStr = "SELECT " + AttributeDetails.ATTRIBUTES_DETAILS_TABLE + ".attributes_id as clusterid," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				AttributeDetails.ATTRIBUTES_DETAILS_TABLE +
				" WHERE attributes_id IN " + attributeidList + " ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, matchingAttributes);
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
		
		ArrayList<String> matchingAds = GraphResource.fetchMatchingAds(tip, log);
		if (matchingAds==null||matchingAds.size()==0) {
			log.popTime();
			return "{\"details\":[]}";
		}

		log.pushTime(" Get clusters for search results");
		HashMap<String,Integer> matchingClusters = TableDB.getInstance().getSimpleClusterCounts(matchingAds, "org");
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
		String sqlStr = "SELECT " + TableDB.CLUSTER_DETAILS_TABLE + ".clusterid," +
				"adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				TableDB.CLUSTER_DETAILS_TABLE +
				" WHERE clusterid IN " + clusteridList + " ORDER BY adcount DESC";
		fetchClusterDetails(sqlStr, log, result, matchingClusters);
		result.append("]}");
		log.popTime();
		return result.toString();
	}

	private void fetchClusterDetails(String sqlStr, TimeLog log, StringBuilder result, HashMap<String,Integer> matchingClusters) {
		TableDB db = TableDB.getInstance();
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
				String clusterid = rs.getString("clusterid");
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
				String adidlist = rs.getString("adidlist");
				String clustername = rs.getString("clustername");
				if (phonelist==null) phonelist = "";
				if (emaillist==null) emaillist = "";
				if (weblist==null) weblist = "";
				if (namelist==null) namelist = "";
				if (ethnicitylist==null) ethnicitylist = "";
				if (timeseries==null) timeseries = "";
				if (locationlist==null) locationlist = "";
				if (sourcelist==null) sourcelist = "";
				if (keywordlist==null) keywordlist = "";
				if (adidlist==null) adidlist = "";
				if (clustername==null) clustername = "";
				result.append("{\"id\":");
				result.append(clusterid); 
				result.append(",\"ads\":");
				result.append(adcount);
				if (clustername!=null) {
					result.append(",\"clustername\":\"");
					result.append(clustername);
					result.append("\"");
				}
				if (matchingClusters!=null) {
					result.append(",\"matches\":");
					result.append(matchingClusters.get(clusterid));
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
				result.append("},\"adidlist\":[");
				result.append(adidlist);
				result.append("]}");
			}
			log.popTime();

		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
	}
	
}

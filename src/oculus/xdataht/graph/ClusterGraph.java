package oculus.xdataht.graph;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import oculus.xdataht.attributes.AttributeDetails;
import oculus.xdataht.attributes.AttributeLinks;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.GraphResult;
import oculus.xdataht.model.RestLink;
import oculus.xdataht.model.RestNode;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.preprocessing.ClusterLinks;
import oculus.xdataht.util.Pair;

import org.json.JSONObject;

public class ClusterGraph {
	private static HashMap<String,HashMap<String,String>> getOrgClusterDetails(HashSet<String> clusterids) {
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		String sqlStr = "SELECT clusterid,adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				TableDB.CLUSTER_DETAILS_TABLE +
				" WHERE clusterid IN (";
		boolean isFirst = true;
		for (String clusterid:clusterids) {
			if (isFirst) isFirst = false;
			else sqlStr += ",";
			sqlStr += clusterid;
		}
		sqlStr += ")";
		HashMap<String,HashMap<String,String>> result = new HashMap<String,HashMap<String,String>>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String clusterid = rs.getString("clusterid");
				String clustername = rs.getString("clustername");
				if (clustername==null) clustername = "";
				HashMap<String, String> details = getDetailsFromSQL(rs);
				result.put(clusterid, details);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
		}
		db.close();
		return result;
	}

	private static HashMap<Integer,HashMap<String,String>> getAttributeDetails(HashSet<Integer> attributeids) {
		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		Statement stmt = null;
		String sqlStr = "SELECT attributes_id,adcount,phonelist,emaillist,weblist,namelist,ethnicitylist,timeseries,locationlist,sourcelist,keywordlist,adidlist,clustername FROM " + 
				AttributeDetails.ATTRIBUTES_DETAILS_TABLE +	" WHERE attributes_id IN (";
		boolean isFirst = true;
		for (Integer attrid:attributeids) {
			if (isFirst) isFirst = false;
			else sqlStr += ",";
			sqlStr += attrid;
		}
		sqlStr += ")";
		HashMap<Integer,HashMap<String,String>> result = new HashMap<Integer,HashMap<String,String>>();
		try {
			stmt = localConn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				Integer clusterid = rs.getInt("attributes_id");
				HashMap<String, String> details = getDetailsFromSQL(rs);
				result.put(clusterid, details);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (SQLException e) {e.printStackTrace();}
		}
		db.close();
		return result;
	}
	
	public static HashMap<String, String> getDetailsFromSQL(ResultSet rs) throws SQLException {
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
		HashMap<String,String> details = new HashMap<String,String>();
		details.put("adcount", "" + adcount);
		details.put("phonelist",phonelist);
		details.put("emaillist",emaillist);
		details.put("weblist",weblist);
		details.put("namelist",namelist);
		details.put("ethnicitylist",ethnicitylist);
		details.put("timeseries",timeseries);
		details.put("locationlist",locationlist);
		details.put("sourcelist",sourcelist);
		details.put("keywordlist",keywordlist);
		details.put("adidlist",adidlist);
		details.put("clustername",clustername);
		return details;
	}
	
	
	@SuppressWarnings("unchecked")
	public static void fetchOrgLinks(HashSet<String> matchingClusters, int[] clusterSizeRange, GraphResult result) {

		// Create links between the first layer and add the second layer
		HashSet<String> ring1 = (HashSet<String>)matchingClusters.clone();
		HashMap<String, HashMap<String,Pair<String, String>>> allClusterLinks = ClusterLinks.getLinks(matchingClusters);
		for (HashMap<String,Pair<String, String>> singleClusterLinks:allClusterLinks.values()) {
			for (String otherid:singleClusterLinks.keySet()) {
				matchingClusters.add(otherid);
			}
		}
		HashSet<String> ring2 = (HashSet<String>)matchingClusters.clone();
		
		// Get all links one more step out
		allClusterLinks = ClusterLinks.getLinks(matchingClusters);
		for (HashMap<String,Pair<String, String>> singleClusterLinks:allClusterLinks.values()) {
			for (String otherid:singleClusterLinks.keySet()) {
				matchingClusters.add(otherid);
			}
		}

		// Get all the details for all the clusters
		HashMap<String, HashMap<String, String>> orgClusterDetails = getOrgClusterDetails(matchingClusters);

		for (Map.Entry<String, HashMap<String, String>> clusterDetailEntry:orgClusterDetails.entrySet()) {
			String clusterid = clusterDetailEntry.getKey();
			HashMap<String,String> details = clusterDetailEntry.getValue();
			RestNode rn = new RestNode();
			rn.setId(clusterid);
			if (ring1.contains(clusterid)) {
				rn.setRing(0);
			} else if (ring2.contains(clusterid)) {
				rn.setRing(1);
			} else {
				rn.setRing(2);
			}
			int count = Integer.parseInt(details.get("adcount"));
			rn.setClusterSize(count);
			rn.setSize(count);
			if (count<clusterSizeRange[0]) clusterSizeRange[0] = count;
			if (count>clusterSizeRange[1]) clusterSizeRange[1] = count;
			HashMap<String,Pair<String, String>> links = allClusterLinks.get(clusterid);
			HashSet<String> linkReasons = new HashSet<String>();

			if (links!=null && links.size()>0) {
				ArrayList<StringMap> outlinks = new ArrayList<StringMap>(links.size());
				for (Map.Entry<String, Pair<String,String>> e:links.entrySet()) {
					StringMap destLink = new StringMap();
					outlinks.add(destLink);
					linkReasons.add(e.getValue().getFirst() + " (" + e.getValue().getSecond() + ")");
					if (clusterid.compareTo(e.getKey())>0) {
						result.addLink(new RestLink(clusterid, e.getKey(), 0, e.getValue().getFirst()));
						destLink.put("id", clusterid + "_" + e.getKey());
						destLink.put("other", e.getKey());
					} else {
						destLink.put("id", e.getKey() + "_" + clusterid);
						destLink.put("other", e.getKey());
					}
				}
				rn.setLinks(outlinks);
			}
			orgClusterAttributes(rn, details, linkReasons);
			result.addNode(rn);
		}
		
	}
	
	/**
	 * Return the first value as the title and other values separated by ,s and \ns from
	 * data of the form key1:count1,key2:count2
	 */
	@SuppressWarnings("rawtypes")
	private static String orgTooltipString(String listStr) {
		if (listStr==null || listStr.length()==0) return "";
		try {
			Map<String,Integer> map = new HashMap<String,Integer>();
			JSONObject jo = new JSONObject("{"+listStr+"}");
			Iterator iter = jo.keys();
			while (iter.hasNext()) {
				String val = (String)iter.next();
				map.put(DataUtil.sanitizeHtml(val), Integer.parseInt(jo.getString(val)));
			}
			String valueStr = "";
			boolean isFirst = true;
			int count = 0;
			for (String key:sortByValue(map).keySet()) {
				if (isFirst) {
					isFirst = false;
					if(key.equals("none")){
						if(map.size()<2) return "";
						else continue;
					}
				} else if(key.equals("none")) {
					continue;
				} else {
					valueStr += "\n";
				}
				int num = map.get(key);
				valueStr += num + "\t" + key;
				count++;
				if (count>10) {
					valueStr += "\n&hellip;";
					break;
				}
			}
			return valueStr;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	private static void setTooltipDetail(String title, String accessor, RestNode rn, HashMap<String, String> details,
			StringMap attributes) {
		String detailList = details.get(accessor);
		String detailStr = orgTooltipString(detailList);
		if(detailStr.length()>0) attributes.put(title, detailStr);
	}
	
	private static void orgClusterAttributes(RestNode rn,
		HashMap<String, String> details, HashSet<String> linkReasons) {
		StringMap attributes = new StringMap();
		rn.setName(details.get("clustername"));
		rn.setLabel(details.get("clustername"));

		setTooltipDetail("Email Addresses", "emaillist", rn, details, attributes);
		setTooltipDetail("Phone Numbers", "phonelist", rn, details, attributes);
		setTooltipDetail("Websites", "weblist", rn, details, attributes);
		String linkReasonsStr = "";
		for (String lr:linkReasons) linkReasonsStr += "\t" + lr + "\n";
		attributes.put("Link Reasons", linkReasonsStr);
		
		rn.setAttributes(attributes);
	}

	private static void attributeAttributes(RestNode rn,
			HashMap<String, String> details){
		StringMap attributes = new StringMap();
		rn.setName(details.get("clustername"));
		rn.setLabel(details.get("clustername"));

		setTooltipDetail("Email Addresses", "emaillist", rn, details, attributes);
		setTooltipDetail("Phone Numbers", "phonelist", rn, details, attributes);
		setTooltipDetail("Websites", "weblist", rn, details, attributes);
		setTooltipDetail("Common Ads", "linkreasonlist", rn, details, attributes);
		
		rn.setAttributes(attributes);
	}


	@SuppressWarnings("unchecked")
	public static void fetchAttributeLinks(HashSet<Integer> matchingClusters,
			int[] clusterSizeRange, GraphResult result) {

		// Create links between the first layer and add the second layer
		HashSet<Integer> ring1 = (HashSet<Integer>)matchingClusters.clone();
		HashMap<Integer, HashMap<Integer,Integer>> allClusterLinks = AttributeLinks.getLinks(matchingClusters);
		for (HashMap<Integer,Integer> singleClusterLinks:allClusterLinks.values()) {
			for (Integer otherid:singleClusterLinks.keySet()) {
				matchingClusters.add(otherid);
			}
		}
		HashSet<String> ring2 = (HashSet<String>)matchingClusters.clone();
		
		// Get all links one more step out
		allClusterLinks = AttributeLinks.getLinks(matchingClusters);
		for (HashMap<Integer,Integer> singleClusterLinks:allClusterLinks.values()) {
			for (Integer otherid:singleClusterLinks.keySet()) {
				matchingClusters.add(otherid);
			}
		}

		// Get all the details for all the clusters
		HashMap<Integer, HashMap<String, String>> orgClusterDetails = getAttributeDetails(matchingClusters);

		for (Map.Entry<Integer, HashMap<String, String>> clusterDetailEntry:orgClusterDetails.entrySet()) {
			Integer clusterid = clusterDetailEntry.getKey();
			HashMap<String,String> details = clusterDetailEntry.getValue();
			RestNode rn = new RestNode();
			rn.setId(""+clusterid);
			if (ring1.contains(clusterid)) {
				rn.setRing(0);
			} else if (ring2.contains(clusterid)) {
				rn.setRing(1);
			} else {
				rn.setRing(2);
			}
			int count = Integer.parseInt(details.get("adcount"));
			rn.setClusterSize(count);
			rn.setSize(count);
			if (count<clusterSizeRange[0]) clusterSizeRange[0] = count;
			if (count>clusterSizeRange[1]) clusterSizeRange[1] = count;
			HashMap<Integer,Integer> links = allClusterLinks.get(clusterid);
			
			if (links!=null && links.size()>0) {
				StringBuilder linkReasonsStr = new StringBuilder(links.size()*20);
				ArrayList<StringMap> outlinks = new ArrayList<StringMap>(links.size());
				boolean isFirst = true;
				for (Map.Entry<Integer,Integer> e:sortByValue(links).entrySet()) {
					
					if(isFirst) {
						linkReasonsStr.append("\""+ orgClusterDetails.get(e.getKey()).get("clustername") + "\":"+e.getValue());
						isFirst = !isFirst;
					} else {
						linkReasonsStr.append(",\""+ orgClusterDetails.get(e.getKey()).get("clustername") + "\":"+e.getValue());
					}
					
					StringMap destLink = new StringMap();
					outlinks.add(destLink);
					
					if (clusterid.compareTo(e.getKey())>0) {
						result.addLink(new RestLink(Integer.toString(clusterid), Integer.toString(e.getKey()), e.getValue(), "ad"));
						destLink.put("id", clusterid + "_" + e.getKey());
						destLink.put("other", Integer.toString(e.getKey()));
					} else {
						destLink.put("id", e.getKey() + "_" + clusterid);
						destLink.put("other", Integer.toString(e.getKey()));
					}
				}
				rn.setLinks(outlinks);
				details.put("linkreasonlist", linkReasonsStr.toString());
			}
			attributeAttributes(rn, details);
			result.addNode(rn);
		}		
	}
	
	private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
			Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
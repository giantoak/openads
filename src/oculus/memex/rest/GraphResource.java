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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import oculus.memex.clustering.AttributeValue;
import oculus.memex.clustering.Cluster;
import oculus.memex.clustering.ClusterAttributes;
import oculus.memex.clustering.ClusterDetails;
import oculus.memex.db.DBManager;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.geo.Geocoder;
import oculus.memex.graph.AttributeLinks;
import oculus.memex.graph.ClusterGraph;
import oculus.memex.graph.ClusterLinks;
import oculus.memex.image.AdImages;
import oculus.memex.image.ImageHistogramHash;
import oculus.xdataht.clustering.ClusterLink;
import oculus.xdataht.clustering.ClusterResults;
import oculus.xdataht.clustering.LinkFilter;
import oculus.xdataht.data.ClusterCache;
import oculus.xdataht.data.TableGraph;
import oculus.xdataht.model.ClusterLevel;
import oculus.xdataht.model.GraphRequest;
import oculus.xdataht.model.GraphResult;
import oculus.xdataht.model.RestFilter;
import oculus.xdataht.model.RestLinkCriteria;
import oculus.xdataht.model.RestNode;
import oculus.xdataht.model.SimpleGraphRequest;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.StringUtil;
import oculus.xdataht.util.TimeLog;

import org.restlet.resource.ResourceException;

@Path("/graph")
public class GraphResource  {

	public void mergeConnectivity(Map<String, List<ClusterLink>> c1, Map<String, List<ClusterLink>> c2) {
		for (String clusterId : c2.keySet()) {
			List<ClusterLink> oldLinks = c1.get(clusterId);
			List<ClusterLink> newLinks = c2.get(clusterId);
			if (oldLinks == null) {
				c1.put(clusterId,newLinks);
			} else {
				for (ClusterLink link : newLinks) {
					oldLinks.add(link);
				}
				c1.put(clusterId, oldLinks);
			}
		}
	}
	
	
	@POST
	@Path("link")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult handlePost(GraphRequest request) throws ResourceException {
		GraphResult result = new GraphResult();
		TimeLog timeLog = new TimeLog();

		timeLog.pushTime("Advanced search (" + request.getDatasetName() + "," + request.getClustersetName() + ")");
		timeLog.pushTime("Setup");
		String datasetName = request.getDatasetName();

		String clustersetName = request.getClustersetName();

		ClusterResults clusterResults = ClusterCache.getResults(datasetName, clustersetName);
					
		ArrayList<RestFilter> restFilters = request.getFilters();
		if (restFilters == null) {
			restFilters = new ArrayList<RestFilter>();
		}
		
		ArrayList<RestLinkCriteria> linkCriteria = request.getLinkCriteria();
		if (linkCriteria == null) {
			linkCriteria = new ArrayList<RestLinkCriteria>();
		}

		timeLog.popTime();
		timeLog.pushTime("Get existing clusters");
		ArrayList<ClusterLevel> existingClusters = request.getExistingClusters();
		Map<String, Integer> existingNodes = new HashMap<String,Integer>();
		
		if (existingClusters != null) {
			for (ClusterLevel level : existingClusters) {
				String id = level.getId();
				Integer ring = level.getLevel();
				existingNodes.put(id, ring);
			}
		}
		
		if (clusterResults != null && linkCriteria != null) {
			
			timeLog.popTime();
			timeLog.pushTime("Get attributes of interest");

			// Get attributes of interest
			ArrayList<String> attributesOfInterest = new ArrayList<String>();
			for (RestLinkCriteria rlc : linkCriteria) {
				for (String attr : rlc.getAttributes()) {
					if (attributesOfInterest.indexOf(attr) == -1) {
						attributesOfInterest.add(attr);
					}
				}
			}

			// Compute connectivity adjacency lists

			timeLog.popTime();
			timeLog.pushTime("Get filters");
			ArrayList<LinkFilter> filters = getFilters(restFilters, attributesOfInterest);

			if (existingNodes.keySet().size() != 0) {
				timeLog.popTime();
				timeLog.pushTime("Fetching related clusters");
				List<String> newAndOldNodeList = getRelatedClusters(datasetName, clusterResults, existingNodes,	linkCriteria);
				
				// Recompute connectivity between all new nodes
				timeLog.popTime();
				timeLog.pushTime("Computing connectivity");
				Map<String, List<ClusterLink>> connectivity = clusterResults.getConnectivity(newAndOldNodeList, datasetName, linkCriteria);
				
				timeLog.popTime();
				timeLog.pushTime("Creating Graph");
				boolean onlyLinkedNodes = request.getOnlyLinkedNodes();
				TableGraph.create(clusterResults, newAndOldNodeList, connectivity, result, onlyLinkedNodes, datasetName, attributesOfInterest, existingNodes);
				
			} else {
				// Standard case, getting a graph
				timeLog.popTime();
				timeLog.pushTime("Filtering clusters");
				List<String> filteredClusters = clusterResults.dbFilter(filters);

				timeLog.popTime();
				timeLog.pushTime("Computing connectivity");
				Map<String, List<ClusterLink>> connectivity = clusterResults.getConnectivity(filteredClusters, datasetName, linkCriteria);
			
				timeLog.popTime();
				timeLog.pushTime("Creating Graph");
				boolean onlyLinkedNodes = request.getOnlyLinkedNodes();
				TableGraph.create(clusterResults, filteredClusters, connectivity, result, onlyLinkedNodes, datasetName, attributesOfInterest, existingNodes);
			}
			timeLog.popTime();
		}
		timeLog.popTime();
		
		return result;
	}

	public static String getPhone(String str) {
		String result = "";
		for (int i=0; i<str.length(); i++) {
			char c = str.charAt(i);
			if ('0'<=c && c<='9') {
				result += c;
			} else if (c=='('||c==')'||c=='-') {
				// do nothing
			} else {
				return null;
			}
		}
		return result;
	}
	
	public static String getLocation(String str) {
		String result = new String(str).toLowerCase();
		result = result.replaceAll("\\s+","");
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection localConn = db.open();
		Pair<Float,Float> pos = Geocoder.geocode(db, localConn, str);
		db.close();
		if (pos==null) return null;
		return result;
	}
	
	@POST
	@Path("simple")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult simpleSearch(SimpleGraphRequest request) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Simple search (" + request.getSearchString() + "," + request.getClusterType() + ")");

		String search = request.getSearchString();
		HashSet<Integer> matchingAds = fetchMatchingAds(search, timeLog);

		if (matchingAds==null || matchingAds.size()==0) {
			timeLog.popTime();
			return new GraphResult();
		}
		
		timeLog.pushTime("Get clusters for search results");
		
		// Fetch the clusters that correspond to the matching ads
		HashSet<Integer> matchingClusters = Cluster.getSimpleClusters(matchingAds);

		timeLog.popTime();
		timeLog.pushTime("Create result nodes");

		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};
		GraphResult result = new GraphResult();

		ClusterGraph.fetchLinks(matchingClusters, clusterSizeRange, result, request.getRingCount());

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		timeLog.popTime();
		
		return result;
	}
	
	@POST
	@Path("getlinkadids")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public String getLinkAdIds(String link, @Context HttpServletRequest request) {
		TimeLog log = new TimeLog();
		log.pushTime("Get Graph node link Ad IDs");
		String[] clusterIDs = link.split("_");
		HashSet<String> result = new HashSet<String>();		
		AttributeDetailsResource adr = new AttributeDetailsResource();
		HashSet<String> ad_ids = new HashSet<String>();
		ArrayList<StringMap> details = adr.handleGet("id", clusterIDs[0], request).getMemberDetails();
		for(int i = 0; i < details.size(); i++) {
			ad_ids.add(details.get(i).get("id"));
		}
		details = adr.handleGet("id", clusterIDs[1], request).getMemberDetails();
		String newID;
		for(int i = 0; i < details.size(); i++) {
			newID = details.get(i).get("id");
			if(ad_ids.contains(newID)) {
				result.add("\"" + details.get(i).get("id") + "\"");
			}
		}
		log.popTime();
		return result.toString();
	}

	static int USE_FULL_TEXT = 0; // 0->unknown, 1->yes, 2->no
	public static boolean useFullText() {
		if (USE_FULL_TEXT==0) {
			MemexHTDB htdb = MemexHTDB.getInstance();
			Connection htconn = htdb.open();
			String sqlStr = "SELECT DISTINCT column_name FROM INFORMATION_SCHEMA.STATISTICS WHERE (table_schema,table_name)=('" + ScriptDBInit._htSchema +"','ads') AND index_type='FULLTEXT'";
			int check = DBManager.getResultCount(htconn, sqlStr, "Full Text Index check");
			htdb.close();
			USE_FULL_TEXT = (check>0)?1:2;
		}
		return USE_FULL_TEXT==1;
	}
	

	public static HashSet<Integer> fetchMatchingAds(String search, TimeLog timeLog) {
		timeLog.pushTime("Fetch matching ads");

		// Search the database for ads matching the search string
		HashSet<Integer> matchingAds = null;
		String phone = getPhone(search);
		if (phone!=null) {
			timeLog.pushTime("Search Phone Numbers");
			matchingAds = MemexOculusDB.getPhoneAds("value like '%"+phone+"%'");
		} else {
			String location = getLocation(search);
			if (location!=null) {
				timeLog.pushTime("Search Region");
				matchingAds = MemexHTDB.getAds("region like '%"+location+"%'");
			}
			if (matchingAds==null || matchingAds.size()==0) {
//				timeLog.pushTime("Search Text");
//				matchingAds = MemexOculusDB.getValueAds("ads_websites", search, false);
//				matchingAds.addAll(MemexOculusDB.getValueAds("ads_emails", search, false));
					if (useFullText()) {
						timeLog.pushTime("Search Full Text");
						matchingAds = MemexHTDB.getAds("match(text,title,email,website) against('"+search+"*' in boolean mode)");
					} else {
						timeLog.pushTime("Search Text Like");
						matchingAds = MemexHTDB.getAds("text like '%"+search+"%' or title like '%"+search+"%' or email like '%"+search+"%' or website like '%"+search+"%'");
					}
			}
		}
		timeLog.popTime();

		timeLog.popTime();
		return matchingAds;
	}

	private static int PHONE_SELECT_BATCH_SIZE = 500;
	public static HashSet<Integer> fetchMatchingAds(String[] phones, TimeLog timeLog) {
		HashSet<Integer> matchingAds = new HashSet<Integer>();
		timeLog.pushTime("Fetch matching ads for " + phones.length + " phone numbers");

		int i=0;
		while (i<phones.length) {
			String whereClause = "value IN (";
			boolean isFirst = true;
			for (int idx = 0; (idx<PHONE_SELECT_BATCH_SIZE) && (i+idx<phones.length); idx++) {
				if (isFirst) isFirst = false;
				else whereClause += ",";
				whereClause += StringUtil.stripNonNumeric(phones[i+idx]);
			}
			i += PHONE_SELECT_BATCH_SIZE;
			whereClause += ")";
			matchingAds.addAll(MemexOculusDB.getPhoneAds(whereClause));
		}
		
		timeLog.popTime();
		return matchingAds;
	}

	@POST
	@Path("cluster")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult clusterGraph(SimpleGraphRequest request) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Cluster graph (" + request.getSearchString() + "," + request.getClusterType() + ")");

		GraphResult result = new GraphResult();

		HashSet<Integer> matchingClusters = new HashSet<Integer>();
		matchingClusters.add(Integer.parseInt(request.getSearchString()));
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};

		ClusterGraph.fetchLinks(matchingClusters, clusterSizeRange, result, request.getRingCount());

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
	}

	@POST
	@Path("image")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult imageGraph(SimpleGraphRequest request) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Image graph (" + request.getSearchString() + "," + request.getClusterType() + ")");

		GraphResult result = new GraphResult();

		String search = request.getSearchString();
		if (search==null) {
			timeLog.popTime();
			return result;
		}
		int imageid = -1;
		try {
			imageid = Integer.parseInt(search);
		} catch (NumberFormatException e) {
			timeLog.popTime();
			return result;
		}
		
		// Find image hash
		String hash = ImageHistogramHash.getHash(imageid);

		// Find all imageids with the hash
		HashSet<Integer> imageids;
		HashSet<Integer> matchingAds;
		if (hash!=null) {
			imageids = ImageHistogramHash.getIds(hash);
		} else {
			imageids = new HashSet<Integer>();
		}
		imageids.add(imageid);
		matchingAds = AdImages.getMatchingAds(imageids);
		
		// Find clusters matching ads
		HashSet<Integer> matchingClusters = Cluster.getSimpleClusters(matchingAds);		
		
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};
		ClusterGraph.fetchLinks(matchingClusters, clusterSizeRange, result, request.getRingCount());

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
	}

	@POST
	@Path("attribute/{attribute}/{value}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult attributeGraph(@PathParam("attribute")String attribute, @PathParam("value")String value) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Attribute graph (" + attribute + "," + value + ")");

		GraphResult result = new GraphResult();

		HashSet<AttributeValue> matchingAttributes = new HashSet<AttributeValue>();
		matchingAttributes.add(new AttributeValue(attribute,value));
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};

		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		HashMap<Integer, AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
		oculusdb.close();
		
		ClusterGraph.fetchAttributeLinks(matchingAttributes, allAttributes, clusterSizeRange, result, 3);

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
	}

	@POST
	@Path("attributeid/{attributeid}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult attributeIdGraph(@PathParam("attributeid")int attributeid) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Attribute graph (" + attributeid + ")");

		GraphResult result = new GraphResult();

		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();
		HashMap<Integer, AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
		oculusdb.close();
		
		HashSet<AttributeValue> matchingAttributes = new HashSet<AttributeValue>();
		AttributeValue primaryAV = allAttributes.get(attributeid);
		if (primaryAV == null) {
			timeLog.popTime();
			return null;
		}
		matchingAttributes.add(primaryAV);
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};

		ClusterGraph.fetchAttributeLinks(matchingAttributes, allAttributes, clusterSizeRange, result, 3);

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
	}

	public static void main(String[] args) {
		invalidateValueStatic("website", "http://www.cloudflare.com/email-protection");
	}

	@POST
	@Path("invalidate/{attribute}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public void invalidateValue(@PathParam("attribute")String attribute, String value) throws ResourceException {
		invalidateValueStatic(attribute, value);
	}

	public static void invalidateValueStatic(String attribute, String value) throws ResourceException {
		TimeLog tl = new TimeLog();
		tl.pushTime("Invalidate " + attribute + " " + value);
		tl.pushTime("Deletes");
		// Fetch ads with the attribute
		// Delete from ads_phones, ads_email, ads_website
		HashSet<Integer> matchingAds;
		String tableName = "ads_phones";
		if (attribute.compareToIgnoreCase("website")==0) {
			tableName = "ads_websites";
			matchingAds = MemexOculusDB.getValueAds("ads_websites", value, true);
			MemexOculusDB.deleteValueAds(tableName, value);
		} else if (attribute.compareToIgnoreCase("email")==0) {
			tableName = "ads_emails";
			matchingAds = MemexOculusDB.getValueAds("ads_emails", value, true);
			MemexOculusDB.deleteValueAds(tableName, value);
		} else {
			matchingAds = MemexOculusDB.getPhoneAds("value=" + value);
			MemexOculusDB.deletePhoneAds("value=" + value);
		}
		ClusterAttributes.deleteValueAds(attribute,value);
		tl.popTime();

		ClusterAttributes attributeToClusters = new ClusterAttributes();

		// Recluster
		tl.pushTime("Update clusters " + matchingAds.size() + " ads");
		HashSet<Integer> clusterids = Cluster.updateClusters(matchingAds, attributeToClusters, tl);
		tl.popTime();
		
		// Calculate cluster details, cluster locations, cluster links for affected clusters
		tl.pushTime("Update details " + clusterids.size() + " clusters " + StringUtil.hashSetToSqlList(clusterids));
		ClusterDetails.updateDetails(clusterids);
		tl.popTime();
		tl.pushTime("Update links");
		ClusterLinks.computeLinks(clusterids, attributeToClusters);
		tl.popTime();

		tl.popTime();
	}

	private void normalizeAndSort(int[] clusterSizeRange, GraphResult result) {
		double range = clusterSizeRange[1]-clusterSizeRange[0];
		for (RestNode rnode : result.getNodes()) {
			int size = (int)(rnode.getSize());
			if (range==0) {
				rnode.setSize(0);
			} else {
				double normalizedSize = (size - clusterSizeRange[0])/range;
				rnode.setSize(normalizedSize);
			}
		}
		Collections.sort(result.getNodes(), new Comparator<RestNode>() {
			public int compare(RestNode o1, RestNode o2) {
				Integer ring1 = (Integer)o1.getRing();
				Integer ring2 = (Integer)o2.getRing();
				if (ring2==ring1) {
					Integer size1 = (Integer)o1.getClusterSize();
					Integer size2 = (Integer)o2.getClusterSize();
					return size2-size1;
				}
				return ring1-ring2;
			}
		});
	}

	public static void updateMap(String id, String fieldStr, HashMap<String,HashSet<String>> map) {
		if (fieldStr==null || fieldStr.length()==0) return;
		String[] values = fieldStr.split(",");
		for (String v:values) {
			if (v==null||v.length()==0) continue;
			HashSet<String> idList = map.get(v);
			if (idList==null) {
				idList = new HashSet<String>();
				map.put(v, idList);
			}
			idList.add(id);
		}
	}


	private ArrayList<LinkFilter> getFilters(ArrayList<RestFilter> restFilters,
			ArrayList<String> attributesOfInterest) {
		ArrayList<LinkFilter> filters = new ArrayList<LinkFilter>();
		for (RestFilter restFilter : restFilters) {
			LinkFilter lf = new LinkFilter(restFilter);
			filters.add(lf);
			if (!(lf.filterAttribute.equals("Cluster Size")||lf.filterAttribute.equals("tag"))) {
				attributesOfInterest.add(lf.filterAttribute);
			}
		}
		return filters;
	}


	private List<String> getRelatedClusters(String datasetName,
			ClusterResults clusterResults, Map<String, Integer> existingNodes,
			ArrayList<RestLinkCriteria> linkCriteria) {
		// Create list from node map
		List<String> existingNodeList = new ArrayList<String>();
		for (String clusterId : existingNodes.keySet()) {
			existingNodeList.add(clusterId);
		}
		
		// Get all clusters that don't exist already
		List<String> otherClusters = clusterResults.filter(existingNodes);
		
		// Get connectivity between other clusters and existing clusters
		System.out.print("\tComputing new connectivity between new nodes and old nodes...");
		Map<String,List<ClusterLink>> newAndOldConnectivity = clusterResults.getConnectivity(otherClusters, existingNodeList, datasetName, linkCriteria, true);
		System.out.println("done");
		
		// Merge existing nodes and nodes that are now connected to find a master list of every node we need to display
		Set<String> newAndOldNodes = new HashSet<String>();
		for (String connectedCluster : newAndOldConnectivity.keySet()) {
			newAndOldNodes.add(connectedCluster);
		}
		for (String oldClusterId : existingNodeList) {
			newAndOldNodes.add(oldClusterId);
		}
		List<String> newAndOldNodeList = new ArrayList<String>();
		newAndOldNodeList.addAll(newAndOldNodes);
		return newAndOldNodeList;
	}

}
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
package oculus.xdataht.rest;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import oculus.xdataht.clustering.ClusterLink;
import oculus.xdataht.clustering.ClusterResults;
import oculus.xdataht.clustering.LinkFilter;
import oculus.xdataht.data.ClusterCache;
import oculus.xdataht.data.DenseDataTable;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.data.TableGraph;
import oculus.xdataht.geocode.Geocoder;
import oculus.xdataht.graph.ClusterGraph;
import oculus.xdataht.model.ClusterLevel;
import oculus.xdataht.model.GraphRequest;
import oculus.xdataht.model.GraphResult;
import oculus.xdataht.model.RestFilter;
import oculus.xdataht.model.RestLink;
import oculus.xdataht.model.RestLinkCriteria;
import oculus.xdataht.model.RestNode;
import oculus.xdataht.model.SimpleGraphRequest;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.util.Pair;
import oculus.xdataht.util.TimeLog;

import org.restlet.resource.ResourceException;

@Path("/graph")
public class GraphResource  {
	private static final int GRAPH_SIZE_LIMIT = 400;


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
		TableDB db = TableDB.getInstance();
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
		timeLog.pushTime("Fetch link columns");
		
		// Fetch the relevant columns from the database
		String clusterType = request.getClusterType();
		DenseDataTable table = TableDB.getInstance().getPreclusterColumns();

		timeLog.popTime();
		timeLog.pushTime("Create phone,email,website,location maps");
		
		// Create maps of value for attribute of interest -> lists of ids
		HashMap<String,HashSet<String>> phoneToClusters = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> emailToClusters = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> websiteToClusters = new HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> clusterToAds = new HashMap<String,HashSet<String>>();
		int idIdx = table.columns.indexOf("id");
		int phoneIdx = table.columns.indexOf("phone_numbers");
		int emailIdx = table.columns.indexOf("email");
		int websiteIdx = table.columns.indexOf("websites");
		int clusterTypeIdx = table.columns.indexOf(clusterType);
		for (String[] row:table.rows) {
			String id = row[idIdx];
			String cluster = row[clusterTypeIdx];
			updateMap(cluster, row[phoneIdx], phoneToClusters);
			updateMap(cluster, row[emailIdx], emailToClusters);
			updateMap(cluster, row[websiteIdx], websiteToClusters);
			updateMap(id, cluster, clusterToAds);
		}
		
		timeLog.popTime();

		String search = request.getSearchString();
		ArrayList<String> matchingAds = fetchMatchingAds(search, timeLog);

		if (matchingAds==null || matchingAds.size()==0) {
			timeLog.popTime();
			return new GraphResult();
		}
		
		timeLog.pushTime(" Get clusters for search results");
		
		// Fetch the clusters that correspond to the matching ads
		ArrayList<String> matchingClusters = TableDB.getInstance().getSimpleClusters(matchingAds, clusterType);

		timeLog.popTime();
		timeLog.pushTime("Create result nodes");
		
		// Create result nodes for search result clusters
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};
		HashMap<String,RestNode> clusterToNode = new HashMap<String,RestNode>();
		GraphResult result = new GraphResult();
		int count = 0;
		for (String cid:matchingClusters) {
			HashSet<String> ads = clusterToAds.get(cid);
			RestNode rn = createResultNode(table, ads, cid, clusterType);
			rn.setRing(0);
			result.addNode(rn);
			clusterToNode.put(cid, rn);
			if (ads.size() > clusterSizeRange[1]) clusterSizeRange[1] = ads.size();
			if (ads.size() < clusterSizeRange[0]) clusterSizeRange[0] = ads.size();
			if (count++>GRAPH_SIZE_LIMIT) break;
		}

		timeLog.popTime();
		timeLog.pushTime("Create links");
		
		createLinks(clusterType, table, phoneToClusters, emailToClusters,
				websiteToClusters, clusterToAds, matchingClusters,
				clusterSizeRange, clusterToNode, result, count);

		timeLog.popTime();
		timeLog.pushTime("Normalize and sort");
		normalizeAndSort(clusterSizeRange, result);
		timeLog.popTime();
		timeLog.popTime();
		
		return result;
	}


	public static ArrayList<String> fetchMatchingAds(String search, TimeLog timeLog) {
		timeLog.pushTime("Fetch matching ads");

		// Search the database for ads matching the search string
		ArrayList<String> matchingAds = null;
		String phone = getPhone(search);
		if (phone!=null) {
			timeLog.pushTime("Search Phone Numbers");
			matchingAds = TableDB.getInstance().getAds("phone_numbers like '%"+phone+"%'");
		} else {
			String location = getLocation(search);
			if (location!=null) {
				timeLog.pushTime("Search Region");
				matchingAds = TableDB.getInstance().getAds("region like '%"+location+"%'");
			}
			if (matchingAds==null || matchingAds.size()==0) {
				timeLog.pushTime("Search Text");
				matchingAds = TableDB.getInstance().getAds("body like '%"+search+"%' or title like '%"+search+"%' or email like '%"+search+"%' or websites like '%"+search+"%'");
			}
		}
		timeLog.popTime();

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

		HashSet<String> matchingClusters = new HashSet<String>();
		matchingClusters.add(request.getSearchString());
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};

		ClusterGraph.fetchOrgLinks(matchingClusters, clusterSizeRange, result);

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
	}

	@POST
	@Path("attributeid/{attributeid}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GraphResult attributeidGraph(@PathParam("attributeid")int attributeid) throws ResourceException {
		TimeLog timeLog = new TimeLog();
		timeLog.pushTime("Attribute id graph (" + attributeid + ")");

		GraphResult result = new GraphResult();

		HashSet<Integer> matchingClusters = new HashSet<Integer>();
		matchingClusters.add(attributeid);
		int[] clusterSizeRange = {Integer.MAX_VALUE, Integer.MIN_VALUE};

		ClusterGraph.fetchAttributeLinks(matchingClusters, clusterSizeRange, result);

		normalizeAndSort(clusterSizeRange, result);
		
		timeLog.popTime();
		
		return result;
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

	private void createLinks(String clusterType, DenseDataTable table,
			HashMap<String, HashSet<String>> phoneToClusters,
			HashMap<String, HashSet<String>> emailToClusters,
			HashMap<String, HashSet<String>> websiteToClusters,
			HashMap<String, HashSet<String>> clusterToAds,
			ArrayList<String> matchingClusters, int[] clusterSizeRange,
			HashMap<String, RestNode> clusterToNode, GraphResult result,
			int count) {
		// Create links between the first layer and add the second layer
		HashSet<String> alreadyProcessed = new HashSet<String>();
		for (int i=0; i<matchingClusters.size()&&i<GRAPH_SIZE_LIMIT; i++) {
			String cid = matchingClusters.get(i);
			HashSet<String> ads = clusterToAds.get(cid);
			RestNode rn = clusterToNode.get(cid);
			
			HashSet<String> linkReasons = new HashSet<String>();
			HashSet<String> linkedPhoneClusters = getLinked(table, cid, ads, "phone_numbers", phoneToClusters, linkReasons);
			HashSet<String> linkedEmailClusters = getLinked(table, cid, ads, "email", emailToClusters, linkReasons);
			HashSet<String> linkedWebsiteClusters = getLinked(table, cid, ads, "websites", websiteToClusters, linkReasons);
			String linkReasonsStr = "";
			for (String lr:linkReasons) linkReasonsStr += lr + "\n";
			rn.getAttributes().put("Link Reasons", linkReasonsStr);
			HashSet<Pair<String,String>> linkedClusters = new HashSet<Pair<String,String>>();
			for (String linkedPhone:linkedPhoneClusters) linkedClusters.add(new Pair<String,String>(linkedPhone,"phone"));
			for (String linkedEmail:linkedEmailClusters) linkedClusters.add(new Pair<String,String>(linkedEmail,"email"));
			for (String linkedWebsite:linkedWebsiteClusters) linkedClusters.add(new Pair<String,String>(linkedWebsite,"website"));
			int linkCount = 0;
			int MAX_LINK_COUNT = 10;
			for (Pair<String,String> linkedCluster:linkedClusters) {
				linkCount++;
				if (linkCount>MAX_LINK_COUNT) break;
				if (linkedCluster.getFirst().equals(cid)) continue;
				if (alreadyProcessed.contains(linkedCluster.getFirst())) {
					StringMap destLink = new StringMap();
					destLink.put("id", linkedCluster.getFirst() + "_" + cid);
					destLink.put("other", cid);
					rn.getLinks().add(destLink);
				} else {
					RestNode otherNode = clusterToNode.get(linkedCluster.getFirst());
					if (otherNode==null) {
						if (count>GRAPH_SIZE_LIMIT) continue;
						if (rn.getRing()==2) continue;
						HashSet<String> oads = clusterToAds.get(linkedCluster.getFirst());
						RestNode on = createResultNode(table, oads, linkedCluster.getFirst(), clusterType);
						on.setRing(rn.getRing()+1);
						result.addNode(on);
						count++;
						clusterToNode.put(linkedCluster.getFirst(), on);
						if (oads.size() > clusterSizeRange[1]) clusterSizeRange[1] = oads.size();
						if (oads.size() < clusterSizeRange[0]) clusterSizeRange[0] = oads.size();
						matchingClusters.add(linkedCluster.getFirst());
					}
					StringMap srcLink = new StringMap();
					srcLink.put("id", cid + "_" + linkedCluster.getFirst());
					srcLink.put("other", linkedCluster.getFirst());
					rn.getLinks().add(srcLink);
					result.addLink(new RestLink(cid, linkedCluster.getFirst(), 0, linkedCluster.getSecond()));
				}
				if (count>GRAPH_SIZE_LIMIT) break;
			}
			alreadyProcessed.add(cid);
			if (count>GRAPH_SIZE_LIMIT) break;
		}
	}
	

	public void unused() {
		// Fetch ads then clusters with matching phone numbers
//		ArrayList<String> matchingPhones = TableDB.getInstance().getSimpleColumn(matchingClusters, clusterType, "phone_numbers");
//		String whereClause = "phone in (";
//		boolean isFirst = true;
//		for (String p:matchingPhones) {
//			if (isFirst) isFirst = false;
//			else whereClause += ",";
//			whereClause += p;
//		}
//		whereClause += ")";
//		ArrayList<String> relatedAds = TableDB.getInstance().getAds(whereClause);
//		ArrayList<String> relatedClusters = TableDB.getInstance().getSimpleClusters(relatedAds, cluster);

		// TODO: 1) get distinct phone,email,website from matching cluster ads
		// TODO: 2) find ads with matching phone,email,website
		// TODO: 3) get clusters for related ads
		// TODO: 4) repeat 1-3
		// TODO: 5) getConnectivity on all nodes
		// TODO: 6) TableGraph.create()
//		ArrayList<RestLinkCriteria> linkCriteria = new ArrayList<RestLinkCriteria>();
		
	}
	

	private HashSet<String> getLinked(DenseDataTable table, String cid, HashSet<String> ads, String fieldName, HashMap<String, HashSet<String>> fieldToClusters, HashSet<String> linkReasons) {
		HashSet<String> values = new HashSet<String>();
		int fieldIdx = table.columns.indexOf(fieldName);
		for (String ad:ads) {
			String fieldStr = table.getRowById(ad)[fieldIdx];
			if (fieldStr==null) continue;
			String[] fieldValues = fieldStr.split(",");
			for (String fieldValue:fieldValues) {
				if (fieldValue.length()>0) values.add(fieldValue);
			}
		}
		HashSet<String> result = new HashSet<String>();
		for (String value:values) {
			HashSet<String> matchingClusters = fieldToClusters.get(value);
			if (matchingClusters!=null) {
				if (matchingClusters.size()>1 || 
					(matchingClusters.size()==1 && (!matchingClusters.contains(cid)))) {
					linkReasons.add(fieldName+"(" + value + ")");
					result.addAll(matchingClusters);
				}
			}
		}
		result.remove(cid);
		return result;
	}


	private RestNode createResultNode(DenseDataTable table, HashSet<String> ads, String cid, String clusterType) {
		RestNode rn = new RestNode();
		rn.setId(cid);
		rn.setClusterSize(ads.size());
		rn.setSize(ads.size());
		StringMap attributes = new StringMap();
		attributes.put("Phone Numbers", TableGraph.createAttributeTooltipString(table, ads, "phone_numbers"));
		attributes.put("Emails", TableGraph.createAttributeTooltipString(table, ads, "email"));
		attributes.put("Websites", TableGraph.createAttributeTooltipString(table, ads, "websites"));
		attributes.put("Locations", TableGraph.createAttributeTooltipString(table, ads, "location"));
		rn.setAttributes(attributes);
		String label = getClusterName(table, ads, clusterType);
		rn.setName(label);
		if (label.length()>20) rn.setLabel(label.substring(0,20));
		else rn.setLabel(label);
		ArrayList<StringMap> links = new ArrayList<StringMap>();
		rn.setLinks(links);
		return rn;
	}

	private String getClusterName(DenseDataTable table, HashSet<String> ads, String clusterType) {
		String adid = ads.iterator().next();
		String[] row = table.getRowById(adid);
		int phoneIdx = table.columns.indexOf("phone_numbers");
		int emailIdx = table.columns.indexOf("email");
		int websiteIdx = table.columns.indexOf("websites");
		int locationIdx = table.columns.indexOf("location");
		if (clusterType.equals("location")) {
			String locationStr = row[locationIdx];
			if (locationStr!=null) {
				String[] locations = locationStr.split(",");
				for (String location:locations) if (location!=null&&location.length()>4) return location;
			}
			return "Unknown";
		}
		String emailStr = row[emailIdx];
		if (emailStr!=null) {
			String[] emails = emailStr.split(",");
			for (String email:emails) if (email!=null&&email.length()>4) return email;
		}
		String phoneStr = row[phoneIdx];
		if (phoneStr!=null) {
			String[] phones = phoneStr.split(",");
			for (String p:phones) if (p!=null&&p.length()>4) return p;
		}
		String webStr = row[websiteIdx];
		if (webStr!=null) {
			String[] webs = webStr.split(",");
			for (String w:webs) if (w!=null&&w.length()>4) return w;
		}
		return "Unknown";
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
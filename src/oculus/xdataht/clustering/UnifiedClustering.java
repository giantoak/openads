package oculus.xdataht.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.Pair;

public class UnifiedClustering {
	
	private static boolean matchesAnyProperty(DataTable table, Integer id1, Integer id2, List<String> propertyList) {
		DataRow row1 = table.getRowById(id1.toString());
		DataRow row2 = table.getRowById(id2.toString());

		for (String prop : propertyList) {
			String value1 = row1.get(prop);
			String value2 = row2.get(prop);
			
			if (value1 != null && value2 != null && value1.equals(value2)) {
				return true;
			}
		}
		return false;
	}
	
	public static ClusterResults clusterByLocation(String datasetName, String paramsKey, String locFieldName) {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);
		
		ArrayList<String> columns = new ArrayList<String>();
		columns.add(locFieldName);
		
		long startTime = System.currentTimeMillis();
		System.out.println("Clustering...");
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, columns);
		long endTime = System.currentTimeMillis();
		System.out.println("\tFetch time: " + (endTime-startTime));
		
		HashMap<String, List<Integer>> placeToAdId = new HashMap<String, List<Integer>>();
		for (DataRow row : dt.rows) {
			Integer adId = Integer.parseInt(row.get("id"));
			if (adId == 0) continue;							// First row is just column names....fix this....
			String commaSeparatedPlaces = row.get(locFieldName);
			String []places = commaSeparatedPlaces.split(",");
			if (places.length > 0) {
				String place = places[0];
				
				List<Integer> similarAds = placeToAdId.get(place);
				if (similarAds == null) {
					similarAds = new ArrayList<Integer>();
				}
				similarAds.add(adId);
				placeToAdId.put(place, similarAds);
			} else {
				List<Integer> singletonList = new ArrayList<Integer>();
				singletonList.add(adId);
				placeToAdId.put("UnknownPlace" + adId, singletonList);
			}
		}
		
		
		// Transfer from clusters to result
		Map<String, Set<String>> clustersById = result.getClustersById();
		int count = 0;
		for (List<Integer> cluster : placeToAdId.values()) {
			String name = count++ + "";
			
			Set<String> setCluster = new HashSet<String>();
			for (Integer id : cluster) {
				setCluster.add(id.toString());
			}
			
			clustersById.put(name, setCluster);
		}		
		return result;
	}
	
	public static ClusterResults cluster(String datasetName, String paramsKey, List<String> primaryAttributes, List<String> secondaryAttributes) {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);

		ArrayList<String> allAttributes = new ArrayList<String>();
		
		// Set up clusterParameters and clusterBy lists
		List<Pair<String, Double>> clusterParameters = result.getClusterParameters();
		for (String attr:secondaryAttributes) {
			clusterParameters.add(new Pair<String,Double>(attr, 1.0));
			allAttributes.add(attr);
		}
		for (String attr : primaryAttributes) {
			clusterParameters.add(new Pair<String,Double>(attr,1.0));
			allAttributes.add(attr);
		}
		
		long startTime = System.currentTimeMillis();
		System.out.println("Clustering...");
		
		// Big data fetch of all the columns in necessary and sufficient
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, allAttributes);

		long endTime = System.currentTimeMillis();
		System.out.println("\tFetch time: " + (endTime-startTime));
		
		

		HashMap<Integer, Integer> adIdToClusterId = new HashMap<Integer,Integer>();
		HashMap<Integer, ArrayList<Integer>> clusterIdtoCluster = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, String> adIdToPrimary = new HashMap<Integer,String>();
		HashMap<String, HashMap<String,ArrayList<Integer>>> attributeToValueToIdList = new HashMap<String, HashMap<String,ArrayList<Integer>>>();
		
		// Bucketize
		for (DataRow row : dt.rows) {
			Integer adId = Integer.parseInt(row.get("id"));
			if (adId == 0) continue;							// First row is just column names....fix this....
			for (String attr : secondaryAttributes) {
				String value = row.get(attr);
				HashMap<String,ArrayList<Integer>> valueToIdList = attributeToValueToIdList.get(attr);
				if (valueToIdList == null) {
					valueToIdList = new HashMap<String, ArrayList<Integer>>();
				}
				
				ArrayList<Integer> idList = valueToIdList.get(value);
				if (idList == null) {
					idList = new ArrayList<Integer>();
				}
				idList.add(adId);
				valueToIdList.put(value, idList);
				
				attributeToValueToIdList.put(attr, valueToIdList);
			}
			
			String primaryString = "";
			for (String attr : primaryAttributes) {
				String value = row.get(attr);
				if (value == null || value.equalsIgnoreCase("null") || value.equalsIgnoreCase("")) {
					primaryString += "null";
				} else {
					primaryString += value;
				}
			}
			adIdToPrimary.put(adId, primaryString);
			HashMap<String, ArrayList<Integer>> valueToIdList = attributeToValueToIdList.get("primary");
			if (valueToIdList == null) {
				valueToIdList = new HashMap<String,ArrayList<Integer>>();
			}
			ArrayList<Integer> idList = valueToIdList.get(primaryString);
			if (idList == null) {
				idList = new ArrayList<Integer>();
			}
			idList.add(adId);
			valueToIdList.put(primaryString, idList);
			
			attributeToValueToIdList.put("primary", valueToIdList);
		}
		
			
		int nextClusterId = 0;
		HashMap<String, ArrayList<Integer>> primaryToIdList = attributeToValueToIdList.get("primary"); 
		
		// Ad must match primary key and one of the secondary keys to be clustered together
		for (DataRow row : dt.rows) {
			Integer adId = Integer.parseInt(row.get("id"));
			if (adId == 0) continue;							// First row is just column names....fix this....
			
			boolean bClustered = false;
			
			// See if it fits into any clusters for people with the same primary attributes
			ArrayList<Integer> similarPrimary = primaryToIdList.get( adIdToPrimary.get(adId));
			for (Integer similarPrimaryId : similarPrimary) {
				Integer similarAdClusterId = adIdToClusterId.get(similarPrimaryId); 	
				// If similarPrimaryId is already clustered and it matches any one of the secondary attributes, we should cluster it
				if ( similarAdClusterId != null && matchesAnyProperty(dt, adId, similarPrimaryId, secondaryAttributes)) {
					ArrayList<Integer> similarCluster = clusterIdtoCluster.get(similarAdClusterId);
					similarCluster.add(adId);
					clusterIdtoCluster.put(similarAdClusterId, similarCluster);
					bClustered = true;
					break;
				}
			}
			
			
			if (!bClustered) {
				ArrayList<Integer> newCluster = new ArrayList<Integer>();
				newCluster.add(adId);
				adIdToClusterId.put(adId, nextClusterId);
				clusterIdtoCluster.put(nextClusterId, newCluster);
				nextClusterId++;
			}	
		}		
		
		// Transfer from clusters to result
		Map<String, Set<String>> clustersById = result.getClustersById();
		int count = 0;
		for (ArrayList<Integer> cluster:clusterIdtoCluster.values()) {
			DataRow firstRowInCluster = dt.getRowById(cluster.get(0).toString());
			String name = null;
			
			int attrCount = 0;
			while(name != null && attrCount < secondaryAttributes.size()) {
				name = firstRowInCluster.get(secondaryAttributes.get(attrCount));
				attrCount++;
			}					
			if (name==null) name = count++ + "";
			if (name.length()>20) name = name.substring(0, 20);
			
			Set<String> setCluster = new HashSet<String>();
			for (Integer id : cluster) {
				setCluster.add(id.toString());
			}
			
			clustersById.put(name, setCluster);
		}
		
		return result;
	}
}

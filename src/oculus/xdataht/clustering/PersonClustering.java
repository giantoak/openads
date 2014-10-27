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

public class PersonClustering {
	
	
	public static ClusterResults clusterTable(String datasetName, String paramsKey, ArrayList<String> clusterBy, ArrayList<String> appearanceAttributes) throws InterruptedException {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);

		// Set up clusterParameters and clusterBy lists
		List<Pair<String, Double>> clusterParameters = result.getClusterParameters();
		for (String attr:clusterBy) {
			clusterParameters.add(new Pair<String,Double>(attr, 1.0));
		}
		
		long startTime = System.currentTimeMillis();
		System.out.println("Clustering...");
		

		ArrayList<String> allAttributes = new ArrayList<String>();
		allAttributes.addAll(clusterBy);
		allAttributes.addAll(appearanceAttributes);
		
		// Big data fetch of all the columns of interest
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, allAttributes , 1000);
		long endTime = System.currentTimeMillis();
		
		String fetchString = "\tFetch time: " + (endTime-startTime); 
		System.out.println(fetchString);

		// Iterate over ads and add them to existing clusters or create new ones
		ArrayList<ArrayList<String>> clusters = new ArrayList<ArrayList<String>>();
		// Attribute Name -> Attribute Value -> Appearance Value -> Ad id list
		HashMap<String,HashMap<String,HashMap<String,ArrayList<String>>>> attrMaps = new HashMap<String,HashMap<String,HashMap<String,ArrayList<String>>>>();
		startTime = System.currentTimeMillis();
		for (DataRow row:dt.rows) {
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			boolean found = false;
			String id = row.get("id");
			String appearanceHash = getAppearanceHash(row, appearanceAttributes);
			for (String attr:clusterBy) {
				HashMap<String,HashMap<String,ArrayList<String>>> attrMap = attrMaps.get(attr);
				if (attrMap==null) {
					attrMap = new HashMap<String,HashMap<String,ArrayList<String>>>();
					attrMaps.put(attr, attrMap);
				}
				String attrVal = row.get(attr);
				if ((!found) && attrVal!=null) {
					attrVal = attrVal.trim().toLowerCase();
					String[] vals = attrVal.split(",");
					for (String val:vals) {
						HashMap<String,ArrayList<String>> attrClusters = attrMap.get(val);
						if (attrClusters!=null) {
							ArrayList<String> attrCluster = attrClusters.get(appearanceHash);
							if (attrCluster!=null) {
								attrCluster.add(id);
								found = true;
								break;
							}
						}
					}
				}
				if (found) break;
			}
			if (!found) {
				ArrayList<String> newCluster = new ArrayList<String>();
				newCluster.add(id);
				clusters.add(newCluster);
				for (String attr:clusterBy) {
					HashMap<String,HashMap<String,ArrayList<String>>> attrMap = attrMaps.get(attr);
					if (attrMap==null) {
						attrMap = new HashMap<String,HashMap<String,ArrayList<String>>>();
						attrMaps.put(attr, attrMap);
					}
					String attrVal = row.get(attr);
					if (attrVal!=null && attrVal.length()>1) {
						attrVal = attrVal.trim().toLowerCase();
						String[] vals = attrVal.split(",");
						for (String val:vals) {
							if (val.length()<3) continue;
							HashMap<String, ArrayList<String>> attrClusters = attrMap.get(val);
							if (attrClusters==null) {
								attrClusters = new HashMap<String, ArrayList<String>>();
								attrMap.put(val, attrClusters);
							}
							attrClusters.put(appearanceHash, newCluster);
						}
					}
				}
			}
		}
		endTime = System.currentTimeMillis();
		System.out.println("\tCluster time: " + (endTime-startTime));
		
		// Transfer from clusters to result
		Map<String, Set<String>> clustersById = result.getClustersById();
		int clusterId = 0;
		for (ArrayList<String> cluster:clusters) {
			clustersById.put(Integer.toString(clusterId), new HashSet<String>(cluster));
			clusterId++;
		}

		endTime = System.currentTimeMillis();
		System.out.println("\tPerson cluster time: " + (endTime-startTime));
		System.out.println("\tClusters: " + clusterId);
		
		return result;
	}

	private static String getAppearanceHash(DataRow row, ArrayList<String> appearanceAttributes) {
		String result = "";
		for (String attr:appearanceAttributes) {
			result += row.get(attr);
		}
		return result;
	}
}

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
import oculus.xdataht.model.ClusteringJobInfo;
import oculus.xdataht.util.Pair;
import scala.actors.threadpool.Arrays;

public class BackpageClustering {
	@SuppressWarnings("unchecked")
	public static ClusterResults clusterTable(String datasetName, String paramsKey, ArrayList<String> clusterBy, ClusteringJobInfo cji) throws InterruptedException {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);

		// Set up clusterParameters and clusterBy lists
		List<Pair<String, Double>> clusterParameters = result.getClusterParameters();
		for (String attr:clusterBy) {
			clusterParameters.add(new Pair<String,Double>(attr, 1.0));
		}
		
		long startTime = System.currentTimeMillis();
		System.out.println("Clustering...");
		cji.appendInfoLine("Fetching data");
		
		
		// Big data fetch of all the columns of interest
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, clusterBy , 1000);
		long endTime = System.currentTimeMillis();
		
		String fetchString = "\tFetch time: " + (endTime-startTime); 
		cji.appendInfoLine(fetchString);
		System.out.println(fetchString);

		// Iterate over ads and add them to existing clusters or create new ones
		ArrayList<ArrayList<String>> clusters = new ArrayList<ArrayList<String>>();
		HashMap<String,HashMap<String,ArrayList<String>>> attrMaps = new HashMap<String,HashMap<String,ArrayList<String>>>();
		startTime = System.currentTimeMillis();
		int count = 0;
		int total = dt.rows.size();
		List<Integer> msgIndicies = Arrays.asList(new Integer[] {total/4, 2*total/4, 3*total/4, total-1});
		double pctComplete = 0.0;
		for (DataRow row:dt.rows) {
			
			if (msgIndicies.indexOf(count) != -1) {
				pctComplete += 0.25;
				cji.appendInfoLine( (pctComplete * 100) + "% complete");
			}
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			boolean found = false;
			String id = row.get("id");
			for (String attr:clusterBy) {
				HashMap<String,ArrayList<String>> attrMap = attrMaps.get(attr);
				if (attrMap==null) {
					attrMap = new HashMap<String,ArrayList<String>>();
					attrMaps.put(attr, attrMap);
				}
				String attrVal = row.get(attr);
				if ((!found) && attrVal!=null) {
					attrVal = attrVal.trim().toLowerCase();
					if (attr.equals("phone_numbers")) {
						String[] nums = attrVal.split(",");
						for (String num:nums) {
							ArrayList<String> attrCluster = attrMap.get(num);
							if (attrCluster!=null) {
								attrCluster.add(id);
								found = true;
								break;
							}
						}
					} else {
						ArrayList<String> attrCluster = attrMap.get(attrVal);
						if (attrCluster!=null) {
							attrCluster.add(id);
							found = true;
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
					HashMap<String,ArrayList<String>> attrMap = attrMaps.get(attr);
					if (attrMap==null) {
						attrMap = new HashMap<String,ArrayList<String>>();
						attrMaps.put(attr, attrMap);
					}
					String attrVal = row.get(attr);
					if (attrVal!=null && attrVal.length()>1) {
						attrVal = attrVal.trim().toLowerCase();
						if (attr.equals("phone_numbers")) {
							String[] nums = attrVal.split(",");
							for (String num:nums) {
								attrMap.put(num, newCluster);
							}
						} else {
							attrMap.put(attrVal, newCluster);
						}
					}
				}
			}
		}
		endTime = System.currentTimeMillis();
		System.out.println("\tCluster time: " + (endTime-startTime));
		
		// Transfer from clusters to result
		Map<String, Set<String>> clustersById = result.getClustersById();
		count = 0;
		for (ArrayList<String> cluster:clusters) {
			DataRow row0 = dt.getRowById(cluster.get(0));
			String name = row0.get("email");
			if (name==null) name = row0.get("phone_numbers");
			if (name==null) name = row0.get("title");
			if (name==null) name = row0.get("location");
			if (name==null) name = row0.get("image_alt");
			if (name==null) name = "Cluster" + (count++);
			if (name.length()>20) name = name.substring(0, 20);
			if (name.equals("")) name = "Cluster" + (count++);
			
			if (clustersById.get(name) != null) {
				name = name + "_01";
			}
			
			clustersById.put(name, new HashSet<String>(cluster));
		}
		
		return result;
	}
}

package oculus.xdataht.clustering;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.preprocessing.ScriptDBInit;
import oculus.xdataht.util.Pair;

public class OrganizationClustering {
	public static ClusterResults clusterTable(String datasetName, String paramsKey, ArrayList<String> clusterBy) throws InterruptedException {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);

		// Set up clusterParameters and clusterBy lists
		List<Pair<String, Double>> clusterParameters = result.getClusterParameters();
		for (String attr:clusterBy) {
			clusterParameters.add(new Pair<String,Double>(attr, 1.0));
		}
		
		long startTime = System.currentTimeMillis();
		System.out.println("Clustering...");
		
		
		// Big data fetch of all the columns of interest
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, clusterBy , 1000);
		long endTime = System.currentTimeMillis();
		
		String fetchString = "\tFetch time: " + (endTime-startTime); 
		System.out.println(fetchString);

		// Iterate over ads and add them to existing clusters or create new ones
		ArrayList<ArrayList<String>> clusters = new ArrayList<ArrayList<String>>();
		HashMap<String,HashMap<String,ArrayList<String>>> attrMaps = new HashMap<String,HashMap<String,ArrayList<String>>>();
		startTime = System.currentTimeMillis();
		for (DataRow row:dt.rows) {
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			String id = row.get("id");
			boolean found = false;
			for (String attr:clusterBy) {
				HashMap<String,ArrayList<String>> attrMap = attrMaps.get(attr);
				if (attrMap==null) {
					attrMap = new HashMap<String,ArrayList<String>>();
					attrMaps.put(attr, attrMap);
				}
				String attrVal = row.get(attr);
				if ((!found) && attrVal!=null) {
					attrVal = attrVal.trim().toLowerCase();
					String[] vals = attrVal.split(",");
					for (String val:vals) {
						ArrayList<String> attrCluster = attrMap.get(val);
						if (attrCluster!=null) {
							attrCluster.add(id);
							found = true;
							break;
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
						String[] vals = attrVal.split(",");
						for (String val:vals) {
							if (val.length()>3)	attrMap.put(val, newCluster);
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
		
		return result;
	}
	
	public static void main(String[] args) {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));
		long start = System.currentTimeMillis();
		ArrayList<String> organizationAttributes = new ArrayList<String>();
		organizationAttributes.add("phone_numbers");
		organizationAttributes.add("email");
		organizationAttributes.add("websites");

		ScriptDBInit.initDB(args);
		System.out.println("Begin precompute org clusters...");
		try {
			ClusterResults organizationResults = OrganizationClustering.clusterTable(ScriptDBInit._datasetName, ScriptDBInit._datasetName + "-organizations", organizationAttributes);
			HashMap<String,String> summary = organizationResults.getSummary();
			String summaryString = "";
			for (String key : summary.keySet()) {
				summaryString += key + " : " + summary.get(key) + '\n'; 
			}
			System.out.println(summaryString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("Done writing precomputed org clusters table in: " + (end-start) + "ms");
	}
	
}

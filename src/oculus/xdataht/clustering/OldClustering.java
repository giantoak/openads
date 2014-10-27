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
import oculus.xdataht.data.TableDistribution;
import oculus.xdataht.model.ClusteringJobInfo;
import oculus.xdataht.util.Pair;
import scala.actors.threadpool.Arrays;

public class OldClustering {

	private static HashMap<String,HashSet<String>> ignoreValues = null;
	public static HashMap<String,HashSet<String>> getIgnoreValues() {
		if (ignoreValues==null) {
			ignoreValues = new HashMap<String,HashSet<String>>();
			HashSet<String> phoneSet = new HashSet<String>();
			phoneSet.add("n/a");
			phoneSet.add("see ad");
			phoneSet.add("read ad");
			phoneSet.add("please read ad");
			phoneSet.add("email only");
			phoneSet.add("email");
			phoneSet.add("given after screening !");
			phoneSet.add("see below");
			ignoreValues.put("phone", phoneSet);
		}
		return ignoreValues;
	}
	
	@SuppressWarnings("unchecked")
	public static ClusterResults clusterTable(String datasetName, String paramsKey, ClusteringJobInfo cji) throws InterruptedException {
		ClusterResults result = new ClusterResults(datasetName, paramsKey);

		// Set up clusterParameters and clusterBy lists
		List<Pair<String, Double>> clusterParameters = result.getClusterParameters();
		ArrayList<String> clusterBy = new ArrayList<String>();
		clusterBy.add("rb_inbox");
		clusterBy.add("phone");
		clusterBy.add("name");
		clusterBy.add("email");
		clusterParameters.add(new Pair<String,Double>("rb_inbox", 1.0));
		clusterParameters.add(new Pair<String,Double>("phone", 1.0));
		clusterParameters.add(new Pair<String,Double>("name", 1.0));
		clusterParameters.add(new Pair<String,Double>("email", 1.0));
		clusterBy.add("ethnicity");
		clusterBy.add("eye_color");
		clusterBy.add("hair_color");
		clusterBy.add("height");
		clusterParameters.add(new Pair<String,Double>("ethnicity", 0.2));
		clusterParameters.add(new Pair<String,Double>("eye_color", 0.2));
		clusterParameters.add(new Pair<String,Double>("hair_color", 0.2));
		clusterParameters.add(new Pair<String,Double>("height", 0.2));
		
		// Get the distribution of each attribute
		HashMap<String,TableDistribution> distributions = new HashMap<String,TableDistribution>();
		for (String attr:clusterBy) {
			TableDistribution dist = TableDB.getInstance().getValueCounts(datasetName, attr);
			distributions.put(attr, dist);
		}
		
		// Big data fetch of all the columns of interest
		DataTable dt = TableDB.getInstance().getDataTableColumns(datasetName, clusterBy);

		// Iterate over ads and add them to existing clusters or create new ones
		ArrayList<ArrayList<String>> clusters = new ArrayList<ArrayList<String>>();
		HashMap<String,ArrayList<String>> inboxMap = new HashMap<String,ArrayList<String>>();
		HashMap<String,ArrayList<String>> phoneMap = new HashMap<String,ArrayList<String>>();
		HashMap<String,ArrayList<String>> nameMap = new HashMap<String,ArrayList<String>>();
		HashMap<String,ArrayList<String>> emailMap = new HashMap<String,ArrayList<String>>();
		int count = 0;
		int total = dt.rows.size();
		List<Integer> msgIndicies = Arrays.asList(new Integer[] {total/4, 2*total/4, 3*total/4, total-1});
		double pctComplete = 0.0;
		long startTime = System.currentTimeMillis();
		for (DataRow row:dt.rows) {
			
			if (msgIndicies.indexOf(count) != -1) {
				pctComplete += 0.25;
				cji.appendInfoLine( (pctComplete * 100) + "% complete");
			}
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			if ((++count)%1000==0)	System.out.println("Processing row: " + count + " clusters: " + clusters.size());
			boolean found = false;
			String inbox = row.get("rb_inbox");
			String phone = row.get("phone");
			String id = row.get("id");
			String name = row.get("name");
			String email = row.get("email");
			String appearance = getRowAppearance(row);
			if (inbox!=null) {
				inbox = inbox.toLowerCase();
				ArrayList<String> inboxCluster = inboxMap.get(inbox);
				if (inboxCluster!=null) {
					inboxCluster.add(id);
					found = true;
				}
			}
			if ((!found) && (phone!=null)) {
				phone = phone.toLowerCase();
				ArrayList<String> phoneCluster = phoneMap.get(phone);
				if (phoneCluster!=null) {
					phoneCluster.add(id);
					found = true;
				}
			}
			if (!found) {
				ArrayList<String> nameCluster = nameMap.get(name);
				if (nameCluster!=null && nameCluster.size()>0) {
					DataRow r2 = dt.getRowById(nameCluster.get(0));
					boolean emailMatch = compareValues(distributions, email, r2.get("email"), "email", 0);
					if (emailMatch) {
						nameCluster.add(id);
						found = true;
					} else {
						boolean appearanceMatch = compareValues(distributions, appearance, getRowAppearance(r2), "appearance", 0);
						if (appearanceMatch) {
							nameCluster.add(id);
							found = true;
						}
					}
				}
			}
			if (!found) {
				ArrayList<String> emailCluster = emailMap.get(name);
				if (emailCluster!=null && emailCluster.size()>0) {
					DataRow r2 = dt.getRowById(emailCluster.get(0));
					boolean appearanceMatch = compareValues(distributions, appearance, getRowAppearance(r2), "appearance", 0);
					if (appearanceMatch) {
						emailCluster.add(id);
						found = true;
					}
				}
			}
			if (!found) {
				ArrayList<String> newCluster = new ArrayList<String>();
				newCluster.add(id);
				clusters.add(newCluster);
				inboxMap.put(inbox, newCluster);
				if (!getIgnoreValues().get("phone").contains(phone)) phoneMap.put(phone, newCluster);
				nameMap.put(name, newCluster);
				emailMap.put(email, newCluster);
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Total time: " + (endTime-startTime));
		
		// Transfer from clusters to result
		Map<String, Set<String>> clustersById = result.getClustersById();
		for (ArrayList<String> cluster:clusters) {
			String name = dt.getRowById(cluster.get(0)).get("rb_inbox");
			clustersById.put(name, new HashSet<String>(cluster));
		}
		
		return result;
	}
	
	private static String getRowAppearance(DataRow row) {
		return row.get("ethnicity")+row.get("eye_color")+row.get("hair_color")+row.get("height");
	}
	
/**
	public static boolean compareDataRow(HashMap<String, TableDistribution> distributions, DataRow r1, DataRow r2) {
		boolean nameMatch = compareValues(distributions, r1, r2, "name", 0);
		boolean emailMatch = compareValues(distributions, r1, r2, "email", 0);
		if (nameMatch&&emailMatch) return true;

		if (nameMatch||emailMatch) {
			if (!compareValues(distributions, r1, r2, "ethnicity", 0)) return false;
			if (!compareValues(distributions, r1, r2, "eye_color", 0)) return false;
			if (!compareValues(distributions, r1, r2, "hair_color", 0)) return false;
			if (!compareValues(distributions, r1, r2, "height", 0)) return false;
		}
		return false;
	}
	private static boolean compareValues(HashMap<String, TableDistribution> distributions, DataRow r1, DataRow r2, String column, int maxCount) {
		String v1 = r1.get(column);
		String v2 = r2.get(column);
		return compareValues(distributions, v1, v2, column, maxCount);
	}
**/
	private static boolean compareValues(HashMap<String, TableDistribution> distributions, String v1, String v2, String column, int maxCount) {
		if (v1==null) return false;
		if (v2==null) return false;
		v1 = v1.toLowerCase();
		HashSet<String> ignoreSet = getIgnoreValues().get(column);
		if (ignoreSet!=null && ignoreSet.contains(v1)) return false;
		v2 = v2.toLowerCase();
		if (maxCount>0) {
			TableDistribution td = distributions.get(column);
			int count = td.distribution.get(v1);
			if (count>maxCount) {
				return false;
			}
		}
		return v1.equals(v2);
	}

}

package oculus.xdataht.clustering;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import oculus.xdataht.data.ClusterCache;
import oculus.xdataht.init.PropertyManager;
import oculus.xdataht.model.ClusterParameter;
import oculus.xdataht.model.ClusterRequest;
import oculus.xdataht.model.ClusteringJobInfo;
import oculus.xdataht.model.StringMap;


public class ClusteringManager {
	
	private static ClusteringManager _instance = null;
	private HashMap<Integer, ClusteringJobInfo> _activeJobs;
	private HashMap<Integer, ClusteringJobInfo> _recentlyCompletedJobs;
	private HashMap<Integer, Thread> _activeThreads;
	
	private ClusteringManager() { 
		_activeJobs = new HashMap<Integer, ClusteringJobInfo>();
		_activeThreads = new HashMap<Integer, Thread>();
		_recentlyCompletedJobs = new HashMap<Integer, ClusteringJobInfo>();
	}
	
	public static ClusteringManager getInstance() {
		if (_instance==null) {
			try {
				_instance = new ClusteringManager();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return _instance;
	}
	
	private synchronized void jobComplete(int handle) {
		ClusteringJobInfo job = _activeJobs.get(handle);
		job.setStatus("completed");
		_recentlyCompletedJobs.put(handle, job);
		_activeJobs.remove(handle);
		_activeThreads.remove(handle);
	}
	
	public HashMap<Integer,ClusteringJobInfo> getActiveJobs() { return _activeJobs; }
	public HashMap<Integer, ClusteringJobInfo> getCompletedJobs() { return _recentlyCompletedJobs; }
	
	public synchronized ClusteringJobInfo createJob(final ClusterRequest request) {
		
		if (_activeThreads.containsKey(request.hashCode()) && _activeJobs.containsKey(request.hashCode())) {
			return null;
		}
		
		final ClusteringJobInfo cji = new ClusteringJobInfo();
		Thread newJobThread = new Thread() { 
			public void run() { 
				
				try {
					cji.appendInfoLine("Beginning clustering job");
					
					// Input/Output names
					String datasetName = request.getDatasetName();
					String clustersetName = request.getClustersetName();
					
					// Get attributes to cluster on
					ArrayList<ClusterParameter> clusterParams = request.getParams();
					ArrayList<String> clusterBy = new ArrayList<String>();
					ArrayList<Double> weights = new ArrayList<Double>();
					for (ClusterParameter p : clusterParams) {
						clusterBy.add(p.getField());
						weights.add(p.getClusterWeight());
					}
					
					String clusterByString = clusterBy.toString();
					String weightsString = weights.toString();
					
					// Generate cluster results if they are not cached
					String paramsKey = datasetName + "-" + clusterByString + "-" + weightsString;
					String clusterCacheKey = clustersetName;
					
					
					ClusterResults clusterResults = null;
					if (ClusterCache.containsResults(clusterCacheKey, paramsKey)) {
						clusterResults = ClusterCache.getResults(datasetName, clusterCacheKey);
						cji.setInfoFromSummary(clusterResults.getSummary());
						jobComplete(request.hashCode());
						return;
					}
					
					// Cluster the table and store it in the DB
					String method = request.getMethod();
					if (method!=null) method.trim();
					cji.appendInfoLine("Clusterset not cached.  Clustering data");
					if (method!=null && method.equalsIgnoreCase("organization")) {
						paramsKey += "-organization";
						clusterResults = BackpageClustering.clusterTable(datasetName, paramsKey, clusterBy, cji);
						cji.appendInfoLine("Clustering phase complete.   Storing results");
						ClusterCache.putResults(clusterCacheKey, clusterResults);
						cji.setInfoFromSummary(clusterResults.getSummary());
					} else if (method!=null && method.equalsIgnoreCase("louvain")) {
						louvainCluster(datasetName);
						StringMap summary = new StringMap();
						summary.put("Louvain", "Complete");
						cji.setInfoFromSummary(summary.getmap());
					} else {
						clusterResults = ClusterResults.clusterTableWithML(datasetName, clusterBy, weights, paramsKey);
						cji.appendInfoLine("Clustering phase complete.   Storing results");
						ClusterCache.putResults(clusterCacheKey, clusterResults);
						cji.setInfoFromSummary(clusterResults.getSummary());
					}
				} catch (InterruptedException e) {
					System.out.println("Clustering process interrupted");
					killJob(cji.getHandle());
					return;
				} catch (Exception e) {
					System.out.println("Clustering process threw exception");
					e.printStackTrace();
					killJob(cji.getHandle());
					return;
				}
				jobComplete(cji.getHandle());
			}
		};
		_activeThreads.put(request.hashCode(), newJobThread);
		_activeJobs.put(request.hashCode(), cji);
		
		cji.setClustersetName(request.getClustersetName());
		cji.setDatasetName(request.getDatasetName());
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		cji.setStartTime(dateFormat.format(date));
		cji.setHandle(request.hashCode());
		cji.setStatus("running");
		
		newJobThread.start();
		return cji;
	}

	public synchronized void requestKill(int handle) {
		Thread jobThread = _activeThreads.get(handle);
		if (jobThread != null) {
			jobThread.interrupt();
		}
	}
	
	private synchronized void killJob(int handle) {
		_activeJobs.remove(handle);
		_activeThreads.remove(handle);
	}

	public synchronized ClusteringJobInfo getJobInfo(int handle) {
		if (_activeJobs.containsKey(handle)) {
			return _activeJobs.get(handle);
		} else if (_recentlyCompletedJobs.containsKey(handle)) {
			ClusteringJobInfo job = _recentlyCompletedJobs.get(handle);
			return job;
		}
		return new ClusteringJobInfo("ERROR - see server log or talk to administrator", "", "", handle, "", "");
	}
	
	public synchronized void removeRecentlyCompletedJob(int handle) {
		if (_recentlyCompletedJobs.containsKey(handle)) {
			_recentlyCompletedJobs.remove(handle);
		}
	}
	
	private void louvainCluster(String dataset) {
		String exec = PropertyManager.getInstance().getProperty(PropertyManager.LOUVAIN_EXECUTABLE);
		String execStr = exec 
				+ " -s localhost"
				+ " -d xdataht"
				+ " -u root"
				+ " -p admin"
				+ " -i " + dataset
				+ " -o " + dataset + "_louvain";
		System.out.println(execStr);
		try {
			Process p = Runtime.getRuntime().exec(execStr);
			p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}

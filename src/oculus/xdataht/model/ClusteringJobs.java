package oculus.xdataht.model;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusteringJobs {
	private HashMap<Integer, ClusteringJobInfo> activeJobs;
	private HashMap<Integer, ClusteringJobInfo> completedJobs;
	
	public ClusteringJobs() { }

	public ClusteringJobs(HashMap<Integer, ClusteringJobInfo> activeJobs,
			HashMap<Integer, ClusteringJobInfo> completedJobs) {
		super();
		this.activeJobs = activeJobs;
		this.completedJobs = completedJobs;
	}

	public HashMap<Integer, ClusteringJobInfo> getActiveJobs() {
		return activeJobs;
	}

	public void setActiveJobs(HashMap<Integer, ClusteringJobInfo> activeJobs) {
		this.activeJobs = activeJobs;
	}

	public HashMap<Integer, ClusteringJobInfo> getCompletedJobs() {
		return completedJobs;
	}

	public void setCompletedJobs(HashMap<Integer, ClusteringJobInfo> completedJobs) {
		this.completedJobs = completedJobs;
	}
	
}

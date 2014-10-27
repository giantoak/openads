package oculus.xdataht.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterResult {
	private StringMap clusteringSummary;
	private boolean cached;
	
	public ClusterResult() { }

	public ClusterResult(StringMap clusteringSummary, boolean cached) {
		super();
		this.clusteringSummary = clusteringSummary;
		this.cached = cached;
	}

	public StringMap getClusteringSummary() {
		return clusteringSummary;
	}

	public void setClusteringSummary(StringMap clusteringSummary) {
		this.clusteringSummary = clusteringSummary;
	}

	public boolean getCached() {
		return cached;
	}

	public void setCached(boolean cached) {
		this.cached = cached;
	}
}

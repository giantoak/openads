package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterDetailsRequest {
	private String clusterId;
	private String clustersetName;
	private ArrayList<RestFilter> filters;
	
	public ClusterDetailsRequest() { }

	public ClusterDetailsRequest(String clusterId, String clustersetName,
			ArrayList<RestFilter> filters) {
		super();
		this.clusterId = clusterId;
		this.clustersetName = clustersetName;
		this.filters = filters;
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getClustersetName() {
		return clustersetName;
	}

	public void setClustersetName(String clustersetName) {
		this.clustersetName = clustersetName;
	}

	public ArrayList<RestFilter> getFilters() {
		return filters;
	}

	public void setFilters(ArrayList<RestFilter> filters) {
		this.filters = filters;
	}
}

package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterRequest {
	private String datasetName;
	private String clustersetName;
	private ArrayList<ClusterParameter> params;
	private String method;
	
	public ClusterRequest() { }

	public ClusterRequest(String datasetName, String clustersetName,
			ArrayList<ClusterParameter> params, String method) {
		super();
		this.datasetName = datasetName;
		this.clustersetName = clustersetName;
		this.params = params;
		this.method = method;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getClustersetName() {
		return clustersetName;
	}

	public void setClustersetName(String clustersetName) {
		this.clustersetName = clustersetName;
	}

	public ArrayList<ClusterParameter> getParams() {
		return params;
	}

	public void setParams(ArrayList<ClusterParameter> params) {
		this.params = params;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String toString() {
		return "DatasetName: " + datasetName + ", ClustersetName: " + clustersetName + ", Params: " + params.toString() + ", Method: " + method;
	}
	
	@Override
	public int hashCode() { 
		return toString().hashCode();
	}
	
}

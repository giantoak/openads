package oculus.xdataht.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterParameter {
	private String field;
	private Double clusterWeight;
	
	public ClusterParameter() {}

	public ClusterParameter(String field, Double clusterWeight) {
		super();
		this.field = field;
		this.clusterWeight = clusterWeight;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Double getClusterWeight() {
		return clusterWeight;
	}

	public void setClusterWeight(Double clusterWeight) {
		this.clusterWeight = clusterWeight;
	}
	
	public String toString() { 
		return "Field = " + field + ", Weight = " + clusterWeight;
	}
}

package oculus.xdataht.model;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusteringJobInfo {
	String info;
	String datasetName;
	String clustersetName;
	Integer handle;
	String startTime;
	String endTime;
	String status;
	
	public ClusteringJobInfo() { }
	
	public ClusteringJobInfo(String info, String datasetName,
			String clustersetName, Integer handle, String startTime,
			String endTime) {
		super();
		this.info = info;
		this.datasetName = datasetName;
		this.clustersetName = clustersetName;
		this.handle = handle;
		this.startTime = startTime;
		this.endTime = endTime;
	}
	public String getInfo() {
		return info;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public void appendInfo(String s) {
		if (this.info == null) {
			this.info = s;
		} else {
			this.info += s;
		}
	}
	public void appendInfoLine(String s) {
		appendInfo(s + "\n");
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
	public Integer getHandle() {
		return handle;
	}
	public void setHandle(Integer handle) {
		this.handle = handle;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getStatus() { 
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}

	public void setInfoFromSummary(HashMap<String, String> summary) {
		String summaryString = "";
		for (String key : summary.keySet()) {
			summaryString += key + " : " + summary.get(key) + '\n'; 
		}
		setInfo(summaryString);
	}
}

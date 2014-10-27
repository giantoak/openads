package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SearchRequest {
	private String datasetName;
	private String outputName;
	private ArrayList<RestFilter> filters;
	
	public SearchRequest() { }

	public SearchRequest(String datasetName,
			ArrayList<RestFilter> filters) {
		super();
		this.datasetName = datasetName;
		this.filters = filters;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public ArrayList<RestFilter> getFilters() {
		return filters;
	}

	public void setFilters(ArrayList<RestFilter> filters) {
		this.filters = filters;
	}

	public String getOutputName() {
		return outputName;
	}

	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}

}

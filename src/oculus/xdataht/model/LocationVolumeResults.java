package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LocationVolumeResults {
	ArrayList<LocationVolumeResult> results;
	
	public LocationVolumeResults() {}

	public LocationVolumeResults(ArrayList<LocationVolumeResult> results) {
		super();
		this.results = results;
	}

	public ArrayList<LocationVolumeResult> getResults() {
		return results;
	}

	public void setResults(ArrayList<LocationVolumeResult> results) {
		this.results = results;
	}
}

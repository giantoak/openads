package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class GeocodeResults {
	ArrayList<GeocodeResult> results;
	
	public GeocodeResults() {}

	public GeocodeResults(ArrayList<GeocodeResult> results) {
		super();
		this.results = results;
	}

	public ArrayList<GeocodeResult> getResults() {
		return results;
	}

	public void setResults(ArrayList<GeocodeResult> results) {
		this.results = results;
	}
}

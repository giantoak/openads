package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class GeocodeRequests {
	ArrayList<GeocodeRequest> requests;
	
	public GeocodeRequests() {}

	public GeocodeRequests(ArrayList<GeocodeRequest> requests) {
		super();
		this.requests = requests;
	}

	public ArrayList<GeocodeRequest> getRequests() {
		return requests;
	}

	public void setRequests(ArrayList<GeocodeRequest> requests) {
		this.requests = requests;
	}
}

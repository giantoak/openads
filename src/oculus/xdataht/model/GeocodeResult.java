package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class GeocodeResult {
	ArrayList<String> ids;
	float lat;
	float lon;
	String location;
	
	public GeocodeResult() { }

	public ArrayList<String> getIds() {
		return ids;
	}

	public void setIds(ArrayList<String> ids) {
		this.ids = ids;
	}

	public float getLat() {
		return lat;
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public float getLon() {
		return lon;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public GeocodeResult(ArrayList<String> ids, float lat, float lon, String location) {
		super();
		this.ids = ids;
		this.lat = lat;
		this.lon = lon;
		this.location = location;
	}

	
}

package oculus.memex.geo;

public class LocationData {
	public String label;
	public float lat;
	public float lon;
	public long time;
	public LocationData(String label, float lat, float lon, long time) {
		this.label = label;
		this.lat = lat;
		this.lon = lon;
		this.time = time;
	}

}

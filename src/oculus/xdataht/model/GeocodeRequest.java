package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class GeocodeRequest {
	private ArrayList<String> ids;
    private String city;
    private String state;
    private String country;
    private String originalData;
    private String field;

	public GeocodeRequest() { }

	public ArrayList<String> getIds() {
		return ids;
	}

	public void setIds(ArrayList<String> ids) {
		this.ids = ids;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getOriginalData() {
		return originalData;
	}

	public void setOriginalData(String originalData) {
		this.originalData = originalData;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public GeocodeRequest(ArrayList<String> ids, String city, String state, String country,
			String originalData, String field) {
		super();
		this.ids = ids;
		this.city = city;
		this.state = state;
		this.country = country;
		this.originalData = originalData;
		this.field = field;
	}
	
}

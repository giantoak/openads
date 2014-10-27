package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DBAttributes {
	private String datasetName;
	private StringList columns;
	
	public DBAttributes() { }
	
	public DBAttributes(String datasetName, ArrayList<String> columns) {
		setcolumns(new StringList(columns));
		setdatasetName(datasetName);
	}
	
	public void setdatasetName(String name) { datasetName = name; }
	public String getdatasetName() { return datasetName; }
	
	public void setcolumns(StringList columns) { this.columns = columns; }
	public StringList getcolumns() { return columns; }
}

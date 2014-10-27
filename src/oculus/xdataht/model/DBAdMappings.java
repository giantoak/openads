package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType( XmlAccessType.FIELD ) 
@XmlRootElement
public class DBAdMappings {
	
	private ArrayList<StringAndStringList> tableToClusterSets;
	private ArrayList<StringAndStringList> tableToColumns;
	
	public DBAdMappings() {}
	
	public DBAdMappings(ArrayList<StringAndStringList> tableToClusterSets, ArrayList<StringAndStringList> tableToColums) {
		setTableToClusterSets(tableToClusterSets);
		setTableToColumns(tableToColums);
	}
	
	public ArrayList<StringAndStringList> getTableToClusterSets() { return tableToClusterSets; }
	public void setTableToClusterSets(ArrayList<StringAndStringList> tableToClusterSets) {
		this.tableToClusterSets = tableToClusterSets;
	}
	
	public ArrayList<StringAndStringList> getTableToColumns() { return tableToColumns; }
	public void setTableToColumns(ArrayList<StringAndStringList> tableToColumns) { this.tableToColumns = tableToColumns; }
}

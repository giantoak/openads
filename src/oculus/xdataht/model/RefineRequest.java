package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RefineRequest {
	private ArrayList<String> columns;
	private ArrayList<String> methods;
	private int ngramSize = 2;
	
	public RefineRequest() {}

	public RefineRequest(ArrayList<String> columns, ArrayList<String> methods) {
		this.setColumns(columns);
		this.setMethods(methods);
	}

	public ArrayList<String> getColumns() {
		return columns;
	}

	public void setColumns(ArrayList<String> columns) {
		this.columns = columns;
	}

	public ArrayList<String> getMethods() {
		return methods;
	}

	public void setMethods(ArrayList<String> methods) {
		this.methods = methods;
	}

	public int getNgramSize() {
		return ngramSize;
	}

	public void setNgramSize(int ngramSize) {
		this.ngramSize = ngramSize;
	}
	
}

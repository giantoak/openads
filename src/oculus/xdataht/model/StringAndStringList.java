package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class StringAndStringList {
	private String string;
	private ArrayList<String> list;
	
	public StringAndStringList(){ }
	
	public StringAndStringList(String string, ArrayList<String> list) {
		this.string = string;
		this.list = list;
	}
	
	public String getString() { return string; }
	public ArrayList<String> getList() { return list; }
	
	public void setString(String string) { this.string = string; }
	public void setList(ArrayList<String> list) { this.list = list; }
}

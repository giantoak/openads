package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterSetResult {
	ArrayList<ClusterDetailsResult> set;
	
	public ClusterSetResult() { }

	public ArrayList<ClusterDetailsResult> getSet() {
		return set;
	}

	public void setSet(ArrayList<ClusterDetailsResult> set) {
		this.set = set;
	}

	
}

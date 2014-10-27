package oculus.xdataht.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClusterDetailsResult {
	private ArrayList<StringMap> memberDetails;
	
	public ClusterDetailsResult() { }

	public ClusterDetailsResult(ArrayList<StringMap> memberDetails) {
		super();
		this.memberDetails = memberDetails;
	}

	public ArrayList<StringMap> getMemberDetails() {
		return memberDetails;
	}

	public void setMemberDetails(ArrayList<StringMap> memberDetails) {
		this.memberDetails = memberDetails;
	}	
}

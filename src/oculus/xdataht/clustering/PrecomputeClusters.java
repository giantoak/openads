package oculus.xdataht.clustering;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oculus.xdataht.data.TableDB;


public class PrecomputeClusters {
	
	static String _datasetName = "ads";
	
	static ArrayList<String> _appearanceAttributes = new ArrayList<String>();
	static ArrayList<String> _organizationAttributes = new ArrayList<String>();
	
	static String _name = "xdataht";
	static String _type = "mysql";
	static String _hostname = "localhost";
	static String _port = "3306"; 
	static String _user = "root";
	static String _pass = "admin";
	
	static String _preclusterTableName = "precluster";
	
	static String _locationFieldName = "city";
	
	public static void initDB(String name, String type, String hostname, String port, 
			String user, String pass) {
		TableDB.getInstance(name, type, hostname, port, user, pass, "");
	}
	
	public static void addAppearanceAttribute(String attr) {
		_appearanceAttributes.add(attr);
	}
	
	public static void addOrganizationAttribute(String attr) {
		_organizationAttributes.add(attr);
	}
	
	public static void setLocationFieldName(String name) {
		_locationFieldName = name;
	}
	
	public static void setDataset(String name) {
		_datasetName = name;
	}
	
	private static void handlePropertyFileLine(String line) {
		String[] pieces = line.split("=");
		if (pieces.length == 2) {
			if (pieces[0].equals("db_type")) {
				
			} else if (pieces[0].equals("db_hostname")) {
				_hostname = pieces[1];
			} else if (pieces[0].equals("db_port")) {
				_port = pieces[1];
			} else if (pieces[0].equals("db_user")) {
				_user = pieces[1];
			} else if (pieces[0].equals("db_pass")) {
				_pass = pieces[1];
			} else  if (pieces[0].equals("db_name")) {
				_name = pieces[1];
			} else  if (pieces[0].equals("dataset")) {
				System.out.println("Using dataset: " + pieces[1]);
				setDataset(pieces[1]);
			} else  if (pieces[0].equals("appearance_attribute")) {
				System.out.println("Adding appearance column: " + pieces[1]);
				addAppearanceAttribute(pieces[1]);
			} else  if (pieces[0].equals("organization_attribute")) {
				System.out.println("Adding organization column: " + pieces[1]);
				addOrganizationAttribute(pieces[1]);
			} else  if (pieces[0].equals("location_attribute")) {
				System.out.println("Using location column: " + pieces[1]);
				setLocationFieldName(pieces[1]);
			} else  if (pieces[0].equals("precluster_table_name")) {
				System.out.println("Writing precluster to table named: " + pieces[1]);
				_preclusterTableName = pieces[1];
			} else  {
				System.out.println("Unknown property: " + pieces[0]);
			}
		} else {
			System.out.println("Malformed property line: " + line);
		}
	}
	
	public static void writePreclusterTable(Map<String, List<String>> results, String preclusterTableName) {
		// Reset precluster table or create it if it doesn't exist
		TableDB.getInstance().open();
		if (TableDB.getInstance().tableExists(preclusterTableName)) {
			System.out.println("Clearing table: " + preclusterTableName);
			TableDB.getInstance().clearTable(preclusterTableName);
		} else {			
			System.out.println("Creating table: " + preclusterTableName);
			TableDB.getInstance().createPreclusterTable(preclusterTableName);
		}
		TableDB.getInstance().close();
		
		try {
			System.out.println("Writing table: " + preclusterTableName);
			TableDB.getInstance().putPreclusterResults(preclusterTableName, results);
		} catch (InterruptedException e) {}
	}
	
	public static Map<String, List<String>> precomputeClusters() {
		System.out.println("Beginning clustering phases");
		
		try {
		long start = System.currentTimeMillis();
		System.out.println("\tPerson Clustering:  ");
		ClusterResults personResults = PersonClustering.clusterTable(_datasetName, _datasetName + "-persons", _organizationAttributes, _appearanceAttributes);
		long end = System.currentTimeMillis();
		System.out.println("Done person clustering in: " + (end-start) + "ms");

		System.out.println("\tOrganization Clustering:  ");
		start = System.currentTimeMillis();
		ClusterResults organizationResults = OrganizationClustering.clusterTable(_datasetName, _datasetName + "-organizations", _organizationAttributes);
		end = System.currentTimeMillis();
		System.out.println("Done organization clustering in: " + (end-start) + "ms");
		
		System.out.println("\tLocation Clustering:  ");
		start = System.currentTimeMillis();
		ArrayList<String> locationAttributes = new ArrayList<String>();
		locationAttributes.add("region");
		ClusterResults locationResults = OrganizationClustering.clusterTable(_datasetName, _datasetName + "-organizations", locationAttributes);
		end = System.currentTimeMillis();
		System.out.println("Done location clustering in: " + (end-start) + "ms");
		
		Map<String, List<String>> adIdToClusterList = new HashMap<String, List<String>>();
		
		Map<String, Set<String>> personResultsMap = personResults.getClustersById();
		Map<String, Set<String>> organizationResultsMap = organizationResults.getClustersById();
		Map<String, Set<String>> locationResultsMap = locationResults.getClustersById();
		
		// Create a set of all known ad ids
		Set<String> allAdIds = new HashSet<String>();
		for (String clusterId : personResultsMap.keySet()) {
			for (String adId : personResultsMap.get(clusterId)) {
				allAdIds.add(adId);
			}
		}		
		for (String clusterId : organizationResultsMap.keySet()) {
			for (String adId : organizationResultsMap.get(clusterId)) {
				allAdIds.add(adId);
			}
		}
		for (String clusterId : locationResultsMap.keySet()) {
			for (String adId : locationResultsMap.get(clusterId)) {
				allAdIds.add(adId);
			}
		}
		
		// Create an empty triple list for each ad id in the result (adIdToClusterList)
		for (String adId : allAdIds) {
			List<String> triple = new ArrayList<String>();
			triple.add("0");
			triple.add("0");
			triple.add("0");
			adIdToClusterList.put(adId, triple);
		}
		
		
		// Update triples for each cluster result map
		for (String clusterId : personResultsMap.keySet()) {
			for (String adId : personResultsMap.get(clusterId)) {
				List<String> triple = adIdToClusterList.get(adId);
				triple.set(0, clusterId);
				adIdToClusterList.put(adId, triple);
			}
		}
		for (String clusterId : organizationResultsMap.keySet()) {
			for (String adId : organizationResultsMap.get(clusterId)) {
				List<String> triple = adIdToClusterList.get(adId);
				triple.set(1, clusterId);
				adIdToClusterList.put(adId, triple);
			}
		}
		for (String clusterId : locationResultsMap.keySet()) {
			for (String adId : locationResultsMap.get(clusterId)) {
				List<String> triple = adIdToClusterList.get(adId);
				triple.set(2, clusterId);
				adIdToClusterList.put(adId, triple);
			}
		}
		
		return adIdToClusterList;
		} catch (Exception e) {}
		return null;
	}
	
	public static void main(String[] args) {
		
		if (args.length > 0) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(args[0]));
				String line;
				
				while ((line = br.readLine()) != null) {
					handlePropertyFileLine(line);
				}
				br.close();
				
			} catch (FileNotFoundException e) {
				System.out.println("File: " + args[0] + " not found.");
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			
			initDB(_name, _type, _hostname, _port, _user, _pass);
		} else {
			addAppearanceAttribute("ethnicity");
			addAppearanceAttribute("age");
	
			addOrganizationAttribute("phone_numbers");
			addOrganizationAttribute("email");
			addOrganizationAttribute("websites");
		}

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		System.out.println("Begin precompute clusters...");
		
		Map<String, List<String>> results = precomputeClusters();
		
		long start = System.currentTimeMillis();
		writePreclusterTable(results, "precluster");
		long end = System.currentTimeMillis();
		System.out.println("Done writing precomputed clusters table in: " + (end-start) + "ms");
	}
}

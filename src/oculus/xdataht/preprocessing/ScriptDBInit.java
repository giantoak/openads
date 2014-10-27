package oculus.xdataht.preprocessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oculus.memex.init.PropertyManager;
import oculus.xdataht.data.TableDB;

public class ScriptDBInit {
	public static String _datasetName = "ads";
	public static String _name = "xdataht";
	public static String _type = "mysql";
	public static String _hostname = "localhost";
	public static String _port = "3306"; 
	public static String _user = "root";
	public static String _pass = "admin";
	public static String _htSchema = "memex_ht";
	public static String _oculusSchema = "memex_oculus";

	public static void initDB(String name, String type, String hostname, String port, 
			String user, String pass) {
		TableDB.getInstance(name, type, hostname, port, user, pass, "");
	}

	private static void handlePropertyFileLine(String line) {
		String[] pieces = line.split("=");
		if (pieces.length == 2) {
			if (pieces[0].equals(PropertyManager.DATABASE_TYPE)) {
			} else if (pieces[0].equals(PropertyManager.DATABASE_HOSTNAME)) {
				_hostname = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_PORT)) {
				_port = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_USER)) {
				_user = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_PASSWORD)) {
				_pass = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_NAME)) {
				_name = pieces[1];
			} else if (pieces[0].equals("dataset")) {
				System.out.println("Using dataset: " + pieces[1]);
				_datasetName = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_HTSCHEMA)) {
				_htSchema = pieces[1];
			} else if (pieces[0].equals(PropertyManager.DATABASE_OCULUSSCHEMA)) {
				_oculusSchema = pieces[1];
			} else  {
				System.out.println("Unknown property: " + pieces[0]);
			}
		} else {
			System.out.println("Malformed property line: " + line);
		}
	}

	public static void initDB(String[] args) {
		readArgs(args);
		initDB(_name, _type, _hostname, _port, _user, _pass);
	}

	public static void readArgs(String[] args) {
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
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}

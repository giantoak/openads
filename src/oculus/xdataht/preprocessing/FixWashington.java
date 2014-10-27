package oculus.xdataht.preprocessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;

import oculus.xdataht.data.TableDB;

public class FixWashington {
	static String _datasetName = "ads";
	
	static String _name = "xdataht";
	static String _type = "mysql";
	static String _hostname = "localhost";
	static String _port = "3306"; 
	static String _user = "root";
	static String _pass = "admin";

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
				_datasetName = pieces[1];
			} else  {
				System.out.println("Unknown property: " + pieces[0]);
			}
		} else {
			System.out.println("Malformed property line: " + line);
		}
	}
	

	public static void initDB(String name, String type, String hostname, String port, 
			String user, String pass) {
		TableDB.getInstance(name, type, hostname, port, user, pass, "");
	}

	public static void fixWashington() {
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "UPDATE ads SET location='Washington, USA' WHERE (location='Washington, DC, USA') AND (websites LIKE '%washington.backpage.com%')";
		db.tryStatement(conn, sqlStr);
		TableDB.getInstance().close();
	}

	public static void main(String[] args) {
		if (args.length > 0) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(args[0]));
				String line;

				while ((line = br.readLine()) != null) {
					handlePropertyFileLine(line.trim());
				}
				br.close();

			} catch (FileNotFoundException e) {
				System.out.println("File: " + args[0] + " not found.");
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}
		initDB(_name, _type, _hostname, _port, _user, _pass);
		fixWashington();
	}
}

package oculus.xdataht.casebuilder;

import java.sql.Connection;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.preprocessing.ScriptDBInit;

public class CaseBuilder {
	
	public static final String CASE_BUILDER_TABLE = "case_builder";
	
	public CaseBuilder(){}
	
	public static void initTable(TableDB db){
		Connection conn = db.open();
		if(db.tableExists(CASE_BUILDER_TABLE)){
			System.out.println(CASE_BUILDER_TABLE + " table exists.");
		} else {
			createCaseBuilderTable(db, conn);
			System.out.println(CASE_BUILDER_TABLE + " table initialized.");
		}
		db.close();
	}
	
	public static void createCaseBuilderTable (TableDB db, Connection conn) {
		try {
			String sqlCreate = 
					"CREATE TABLE `"+CASE_BUILDER_TABLE+"` (" +
						  "id INT(11) NOT NULL," +
						  "is_attribute BOOLEAN NOT NULL," +
						  "case_name VARCHAR(32) NOT NULL," +
						  "PRIMARY KEY(id, is_attribute, case_name) )";
			db.tryStatement(conn, sqlCreate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ScriptDBInit.readArgs(args);
		TableDB db = TableDB.getInstance("xdataht", ScriptDBInit._type, ScriptDBInit._hostname, ScriptDBInit._port, ScriptDBInit._user, ScriptDBInit._pass, "");
		initTable(db);	
	}
}

/**
 * Copyright (c) 2013 Oculus Info Inc.
 * http://www.oculusinfo.com/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package oculus.xdataht.rest;

import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.data.TableDistribution;
import oculus.xdataht.model.DBAdMappings;
import oculus.xdataht.model.DBAttributes;
import oculus.xdataht.model.Distribution;
import oculus.xdataht.model.StringAndStringList;
import oculus.xdataht.model.StringList;


@Path("/data")
public class DataResource {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("clusterSets")
	@Produces({MediaType.APPLICATION_JSON})
	public DBAdMappings getClusterSets() {	
		// Get a mapping from table name to a list of cluster sets associated with that table
		HashMap<String, ArrayList<String>> tableNameToClusterSets = TableDB.getInstance().getClusterSetMapping();
				
		ArrayList<String> tables = TableDB.getInstance().getTableNames(TableDB.DataTableType.AD);
		ArrayList<String> widetables = TableDB.getInstance().getTableNames(TableDB.DataTableType.WIDE);
		tables.addAll(widetables);
		for (String table:tables) {
			ArrayList<String> clusters = tableNameToClusterSets.get(table);
			if (clusters==null) tableNameToClusterSets.put(table, new ArrayList<String>());
		}
		
		// Get a mapping from table name to a list of columns for that table
		HashMap<String, ArrayList<String>> tableNameToColumnName = new HashMap<String,ArrayList<String>>();
		for (String tableName : tables) {
			ArrayList<String> columnNames = TableDB.getInstance().getColumns(tableName);
			tableNameToColumnName.put(tableName, columnNames);
		}
		
		ArrayList<StringAndStringList> datasetToClusterSets = new ArrayList<StringAndStringList>();
		for (String tableName : tables) {
			StringAndStringList instance = new StringAndStringList(tableName, tableNameToClusterSets.get(tableName));
			if (TableDB.getInstance().louvainExists(tableName)) instance.getList().add("louvain");
			datasetToClusterSets.add(instance);
		}
		
		ArrayList<StringAndStringList> datasetToColumnNames = new ArrayList<StringAndStringList>();
		for (String tableName : tables) {
			StringAndStringList instance = new StringAndStringList(tableName, tableNameToColumnName.get(tableName));
			datasetToColumnNames.add(instance);
		}
		
		DBAdMappings result = new DBAdMappings(datasetToClusterSets, datasetToColumnNames);
		return result;
	}
	
	@GET
	@Path("attributes/{datasetName}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public DBAttributes getAttributes(@PathParam("datasetName") String datasetName) {
		ArrayList<String> columns = TableDB.getInstance().getColumns(datasetName);
		DBAttributes attributes = new DBAttributes(datasetName, columns);
		return attributes;
	}
	
	@GET
	@Path("distribution/{datasetName}/{columnName}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ArrayList<Distribution> getDistribution(@PathParam("datasetName") String datasetName, @PathParam("columnName") String columnName) { 		
		ArrayList<Distribution> dist = TableDistribution.getDistribution(datasetName, columnName);
		return dist;
	}
	
	@GET
	@Path("values/{datasetName}/{attributeName}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public StringList getValues(@PathParam("datasetName") String datasetName, @PathParam("attributeName") String columnName) {
		ArrayList<String> values = TableDB.getValues(datasetName, columnName);
		return new StringList(values);
	}
	
}

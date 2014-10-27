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

import java.sql.SQLException;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.init.PropertyManager;
import oculus.xdataht.model.RefineRequest;

import org.restlet.resource.ResourceException;

import com.sotera.textrefine.RefinedTableUpdater;


@Path("/refine")
public class TextRefineResource {
	@Context
	UriInfo _uri;
	
	@POST
	@Path("{datasetName}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public String handlePost(RefineRequest request, @PathParam("datasetName") String datasetName) throws ResourceException {
		Properties conf = new Properties();
		PropertyManager pm = PropertyManager.getInstance();
    	conf.setProperty("server", "localhost");
    	conf.setProperty("database", pm.getProperty(PropertyManager.DATABASE_NAME, "xdataht"));
    	conf.setProperty("writeTable", datasetName+"_refine");
    	conf.setProperty("readTable", datasetName);
    	conf.setProperty("user", pm.getProperty(PropertyManager.DATABASE_USER, "root"));
    	conf.setProperty("password", pm.getProperty(PropertyManager.DATABASE_PASSWORD, "admin"));

    	String columnStr = "";
    	String typeStr = "";
    	boolean isFirst = true;
    	for (String column:request.getColumns()) {
    		if (isFirst) isFirst = false;
    		else {
    			columnStr += ", ";
    			typeStr += ", ";
    		}
    		columnStr += column;
    		typeStr += "text";
    	}
    	conf.setProperty("refineColumns", columnStr);
    	conf.setProperty("refineColumns.type", typeStr);

    	String methodStr = "";
    	isFirst = true;
    	for (String method:request.getMethods()) {
    		if (isFirst) isFirst = false;
    		else methodStr += ", ";
    		methodStr += method;
    	}
    	conf.setProperty("hashCollision.methods", methodStr);
    	conf.setProperty("ngram.size", request.getNgramSize() + "");

/**
      	CREATE  TABLE `xdataht`.`ads_refine` (

    			  `id` INT NOT NULL ,

    			  PRIMARY KEY (`id`) );    	
 */
    	
    	try {
			RefinedTableUpdater.execute(conf);
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	return "done";
	}
}

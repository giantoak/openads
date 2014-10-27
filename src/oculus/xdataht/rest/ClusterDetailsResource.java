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
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
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

import oculus.xdataht.data.ClusterCache;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.StringMap;

@Path("/clusterDetails")
public class ClusterDetailsResource  {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("{datasetName}/{clustersetName}/{clusterId}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusterDetailsResult handleGet(@PathParam("clustersetName") String datasetName, @PathParam("clustersetName") String clustersetName, @PathParam("clusterId") String clusterId) {
		ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(ClusterCache.getClusterDetails(datasetName, clustersetName, clusterId));
		ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
		for (HashMap<String,String> map : details) {
			serializableDetails.add( new StringMap(map));
		}
		return new ClusterDetailsResult(serializableDetails);		
	}

}

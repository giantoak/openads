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

import java.sql.Connection;
import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.geocode.Geocoder;
import oculus.xdataht.model.GeocodeRequest;
import oculus.xdataht.model.GeocodeRequests;
import oculus.xdataht.model.GeocodeResult;
import oculus.xdataht.model.GeocodeResults;
import oculus.xdataht.util.Pair;

import org.restlet.resource.ResourceException;


@Path("/geocode")
public class GeocodeResource {
	@Context
	UriInfo _uri;
	
	@POST
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public GeocodeResults handlePost(GeocodeRequests requests) throws ResourceException {

		TableDB db = TableDB.getInstance();
		Connection localConn = db.open();
		
		ArrayList<GeocodeResult> result = new ArrayList<GeocodeResult>();
		for (GeocodeRequest gr : requests.getRequests()) {
			GeocodeResult r = geocodeRequest(db, localConn, gr);
			if (r!=null) result.add(r);
		}
		db.close();
		return new GeocodeResults(result);		
	}

	private GeocodeResult geocodeRequest(TableDB db, Connection localConn, GeocodeRequest gr) {
		Pair<Float,Float> pos = null;
		if (gr.getCity()!=null) {
			pos = Geocoder.geocode(db, localConn, gr.getCity());
		} 
		if (pos==null && gr.getState()!=null) {
			pos = Geocoder.geocode(db, localConn, gr.getState());
		}
		if (pos==null) return null;
		return new GeocodeResult(gr.getIds(), pos.getFirst(), pos.getSecond(), gr.getOriginalData());
	}
}
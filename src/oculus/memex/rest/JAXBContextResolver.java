package oculus.memex.rest;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.ClusterParameter;
import oculus.xdataht.model.ClusterRequest;
import oculus.xdataht.model.DBAdMappings;
import oculus.xdataht.model.Distribution;
import oculus.xdataht.model.GraphResult;
import oculus.xdataht.model.StringAndStringList;
import oculus.xdataht.model.StringList;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

@Provider
@SuppressWarnings("rawtypes")
public class JAXBContextResolver implements ContextResolver<JAXBContext> {
    private JAXBContext context;
	private Class[] types = { ClusterDetailsResult.class, ClusterParameter.class, ClusterRequest.class, DBAdMappings.class, Distribution.class, GraphResult.class, StringAndStringList.class, StringList.class };

    public JAXBContextResolver() throws Exception {
		this.context = new JSONJAXBContext(JSONConfiguration
				.mapped()
				.nonStrings("clusterWeight", "size", "clusters")
				.arrays("params", "tableToColumns", "tableToClusterSets", "list", "nodes", "links", "memberDetails").build(), types);
    }

    public JAXBContext getContext(Class<?> objectType) {
    	for (Class type : types) {
    		if (type == objectType) {
    			return context;
    		}
    	}
    	return null;
    }

}

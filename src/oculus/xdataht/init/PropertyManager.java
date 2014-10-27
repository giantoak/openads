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
package oculus.xdataht.init;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PropertyManager {

	private static PropertyManager instance;
	private static String filename = "xdataht.properties";
	private Map<String, String> pMap;

	public static final String DATABASE_TYPE = "database_type";
	public static final String DATABASE_HOSTNAME = "database_host";
	public static final String DATABASE_PORT = "database_port";
	public static final String DATABASE_USER = "database_user";
	public static final String DATABASE_PASSWORD = "database_password";
	public static final String DATABASE_NAME = "database_name";
	public static final String DATA_DIRECTORY = "data_dir";

	public static final String LOUVAIN_EXECUTABLE = "louvain.executable";
	
	@SuppressWarnings("unchecked")
	private PropertyManager() {
		pMap = new HashMap<String, String>();
		Properties props = ReadProperties(filename);
		for (Enumeration<String> e = (Enumeration<String>) props.propertyNames(); e.hasMoreElements(); ) {
			String key = e.nextElement();
			pMap.put(key, props.getProperty(key));
		}
	}

	public static PropertyManager getInstance() {
		if (instance == null)
			instance = new PropertyManager();
		return instance;
	}
	
	public String getProperty(String key) {
		return instance.pMap.get(key);
	}
	
	public String getProperty(String key, String defaultVal) {
		String val = instance.pMap.get(key);
		return val == null ? defaultVal : val;
	}
	
	public List<String> getPropertyArray(String key) {
		String joined = getProperty(key);
		String[] split = joined.split(",");
		List<String> list = new ArrayList<String>();
		for (String s : split) {
			list.add(s);
		}
		return list;
	}
	
	public Set<String> getPropertySet(String key) {
		Set<String> set = new HashSet<String>();
		String joined = getProperty(key);
		if (joined!=null) {
			String[] split = joined.split(",");
			for (String s : split) {
				set.add(s);
			}
		}
		return set;		
	}
	
	private Properties ReadProperties(String filename) {		
		String resourceLocation = this.getClass().getResource("").getFile();
		if (resourceLocation.startsWith("file:/")) {
			resourceLocation = resourceLocation.substring("file:/".length());
		}
		int idx = resourceLocation.indexOf("WEB-INF");
		if (idx>0) {
			resourceLocation = resourceLocation.substring(0,idx);
		} else {
			idx = resourceLocation.indexOf("classes");
			if (idx>0) {
				resourceLocation = resourceLocation.substring(0,idx+7);
			}
		}
		String defaultPath = resourceLocation + "/" + filename;
		String persistentPath = System.getProperty("catalina.base") + "/conf/" + filename;
		
		File defaultFile = new File(defaultPath);
		File persistentFile = new File(persistentPath);
		File linuxFile = new File("/srv/"+filename);
		
		// If there is no persistent file in configuration directory, look at the default file
		Properties props = new Properties();
		try {
			if ( linuxFile.exists() ) {
				props.load(new FileInputStream(linuxFile));
			} else if (persistentFile.exists()) {
				props.load(new FileInputStream(persistentFile));
			} else {
				props.load(new FileInputStream(defaultFile));
			}
		} catch (IOException e) {}
		return props;
	}
	
	
}


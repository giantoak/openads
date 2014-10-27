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

import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import oculus.xdataht.casebuilder.CaseBuilder;
import oculus.xdataht.data.TableDB;


public class DBInitListener implements ServletContextListener {
	private static final Logger logger = Logger.getLogger(DBInitListener.class.getName());
	
	public void contextInitialized(ServletContextEvent sce) {
		PropertyManager spm = PropertyManager.getInstance();
		String name = spm.getProperty(PropertyManager.DATABASE_NAME, "xdataht");
		String type = spm.getProperty(PropertyManager.DATABASE_TYPE, "mysql");
		String hostname = spm.getProperty(PropertyManager.DATABASE_HOSTNAME, "localhost");
		String port = spm.getProperty(PropertyManager.DATABASE_PORT, "3306");
		String user = spm.getProperty(PropertyManager.DATABASE_USER, "root");
		String pass = spm.getProperty(PropertyManager.DATABASE_PASSWORD, "admin");
		String dataDir = spm.getProperty(PropertyManager.DATA_DIRECTORY, "/srv");
		TableDB db = TableDB.getInstance(name, type, hostname, port, user, pass, dataDir);
		if (db!=null) {
			logger.info("INITIALIZED DATABASE: " + user + "@" + hostname + ":" + port + "");
			try {
				db.cache();
			} catch (Exception e) {
				logger.error("Exception caching data to DB: ", e);
			}
		} else {
			logger.error("FAILED TO INITIALIZE DATABASE");
		}
		CaseBuilder.initTable(db);
	}
	
	public void contextDestroyed(ServletContextEvent sce) {
		// Manually deregister our JDBC driver
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                DriverManager.deregisterDriver(driver);
                logger.info(String.format("deregistering jdbc driver: %s", driver));
            } catch (SQLException e) {
            	logger.error(String.format("Error deregistering driver %s", 
            			driver), e);
            }

        }
        
		// Workaround a MySQL bug - IF we're using it. Use reflection to avoid
		// dependency.
		try {
			Class<?> cls = Class.forName("com.mysql.jdbc.AbandonedConnectionCleanupThread");
			Method mth = (cls == null ? null : cls.getMethod("shutdown"));
			if (mth != null) {
				logger.info("MySQL connection cleanup thread shutdown");
				mth.invoke(null);
				logger.info("MySQL connection cleanup thread shutdown successful");
			}
		} catch (Throwable thr) {
			logger.error("[ER] Failed to shutdown SQL connection cleanup thread: ", thr);
		}
	}
}

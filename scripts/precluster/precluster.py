from jpype import *
jvmPath = getDefaultJVMPath() 

# DB credentials
db_type = "mysql";
db_hostname = "localhost";
db_port = "3306";
db_user = "root";
db_pass = "admin";
db_name = "xdataht";

# Specify classpath to ht project
classpath='./xdataht.jar;./mysql-connector-java-5.1.25.jar;json-20090211.jar'
startJVM(jvmPath,'-Djava.class.path=%s' % classpath)

# Load the package
ClusteringPacakge = JPackage('oculus').xdataht.clustering
Class_PrecomputeClusters = ClusteringPacakge.PrecomputeClusters


# initialize DB connection
Class_PrecomputeClusters.initDB(db_name, db_type, db_hostname, db_port, db_user, db_pass);

# Set dataset
Class_PrecomputeClusters.setDataset("ads_tiny");

#Set up clustering attributes
Class_PrecomputeClusters.addAppearanceAttribute("ethnicity");
Class_PrecomputeClusters.addAppearanceAttribute("eye_color");
Class_PrecomputeClusters.addAppearanceAttribute("hair_color");

Class_PrecomputeClusters.addOrganizationAttribute("email");
Class_PrecomputeClusters.addOrganizationAttribute("phone");
Class_PrecomputeClusters.addOrganizationAttribute("website");

Class_PrecomputeClusters.setLocationFieldName("city");      # comma separated list of places

# Invoke preclustering 
unifiedResults = Class_PrecomputeClusters.precomputeClusters()
 
# print unifiedResults
testTriple = unifiedResults['4528']
print testTriple                        # displays the cluster triple (personId,orgId,locId) for ad 4528

#write the table to the DB
Class_PrecomputeClusters.writePreclusterTable(unifiedResults, "precluster")                            


shutdownJVM() 
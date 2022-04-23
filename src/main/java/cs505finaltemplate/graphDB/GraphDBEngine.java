package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.ArrayList;

public class GraphDBEngine {

    public OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    public ODatabaseSession db = orient.open("test", "root", "rootpwd");
    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard

        clearDB(db);

        //create classes
        OClass patient = db.getClass("patient");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }

        OVertex patient_0 = createPatient("mrn_0");
        OVertex patient_1 = createPatient("mrn_1");
        OVertex patient_2 = createPatient("mrn_2");
        OVertex patient_3 = createPatient("mrn_3");

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts("mrn_0");

//        db.close();
//        orient.close();
    }

    public void createContact(OVertex patient_1, OVertex patient_2) {
        OEdge edge = patient_1.addEdge(patient_2, "contact_with");
        edge.save();
    }
    public OVertex createPatient(String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    }

    public ArrayList<String> getContacts(String patient_mrn) {

        ArrayList<String> contacts = new ArrayList<>();
        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            contacts.add(item.getProperty("patient_mrn"));
//            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        return contacts;
    }

    public boolean isPatient(String patient_mrn) {
        String query = "select from patient where patient_mrn = " + patient_mrn;
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            if (item.isVertex()) {
                System.out.println("found patient: " + item.getProperty("patient_mrn"));
                rs.close();
                return true;
            }
        }
        rs.close();
        return false;
    }

    public OVertex getPatient(String patient_mrn) {
        String query = "select from patient where patient_mrn = " + patient_mrn;
        OResultSet rs = db.query(query);

        OVertex patient = null;

        while (rs.hasNext()) {
            OResult item = rs.next();
            if (item.isVertex()) {
                patient = item.getVertex().get();
                break;
            }
        }
        rs.close();
        return patient;
    }

    private void clearDB(ODatabaseSession db) {

        String query = "DELETE VERTEX FROM patient";
        db.command(query);

    }

}

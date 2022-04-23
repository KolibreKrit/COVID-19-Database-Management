package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;
import jdk.nashorn.internal.objects.annotations.Getter;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeam() {
        String responseString = "{}";
        try {

            Map<String,String> responseMap = new HashMap<>();
            if(Launcher.cepEngine != null) {

                responseMap.put("app_status_code","0");
                responseMap.put("Team_members_sids", "[91222301]");
                responseMap.put("team_name", "Oceans");


            } else {
                responseMap.put("success", Boolean.FALSE.toString());
                responseMap.put("status_desc","CEP Engine is null!");
            }

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try {
            Launcher.graphDBEngine.db.activateOnCurrentThread();
            int success = 1;
            Map<String,String> responseMap = new HashMap<>();
            Launcher.alerts = new ArrayList<>();
            Launcher.CEPList = new HashMap<>();
            if (Launcher.graphDBEngine != null) {
                Launcher.graphDBEngine.clearDB();
            }
            responseMap.put("reset_status_code", String.valueOf(success));

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLastCEP(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
//            Map<String,String> responseMap = new HashMap<>();
//            responseMap.put("lastoutput",Launcher.lastCEPOutput);
//            responseString = gson.toJson(responseMap);
            responseString = gson.toJson(Launcher.CEPList);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            String zipList = "[";
            int i = 1;
            int alertLen = Launcher.alerts.size();
            for (String zipCode : Launcher.alerts) {
                zipList += zipCode;
                if (i < alertLen) {
                    zipList += ",";
                }
                i++;
            }
            zipList += "]";
            responseMap.put("ziplist", zipList);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            Integer inAlert = 0;
            if (Launcher.alerts.size() >= 5) {
                inAlert = 1;
            }
            responseMap.put("state_status", String.valueOf(inAlert));
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {
            //generate a response
            Launcher.graphDBEngine.db.activateOnCurrentThread();
            Map<String, String> responseMap = new HashMap<>();
            ArrayList<String> contacts = Launcher.graphDBEngine.getContacts(mrn);
            String contactList = "[";
            int i = 1;
            int contactLen = contacts.size();
            for (String contact : contacts) {
                contactList += contact;
                if (i < contactLen) {
                    contactList += ",";
                }
                i++;
            }
            contactList += "]";
            responseMap.put("contact_list", contactList);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}

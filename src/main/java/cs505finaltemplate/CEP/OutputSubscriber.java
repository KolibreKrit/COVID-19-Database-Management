package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.Topics.TestingData;
import cs505finaltemplate.Topics.ZipData;
import cs505finaltemplate.Topics.ZipInfo;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;
    private Gson gson;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    final Type typeListZipData = new TypeToken<List<ZipData>>(){}.getType();

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
//            List<ZipData> incomingList = gson.fromJson(String.valueOf(msg), typeListZipData);
//            Launcher.alerts = new ArrayList<>();
//            for (ZipData zipData : incomingList) {
//                for (ZipData prevData : Launcher.CEPList) {
//                    for (ZipInfo event : zipData.events) {
//                        for (ZipInfo prevEvent : prevData.events) {
//                            if (event.zip_code.equals(prevEvent.zip_code)) {
//                                Launcher.alerts.add(event.zip_code);
//                            }
//                        }
//                    }
//                }
//            }
//            Launcher.CEPList = incomingList;
            Launcher.lastCEPOutput = String.valueOf(msg);
            Map<String, Integer> tempList = new HashMap<>();
            String[] zipCodes = String.valueOf(msg).split("zip_code\":\"");
            int i = 0;
            String zip = "";
            for (String zipCode : zipCodes) {
                String[] sstr = zipCode.split("count\":");
                for (String unit : sstr) {
                    if (i != 0) {
                        if (i % 2 == 1) {
                            zip = unit.split("\"")[0];
                        }
                        else {
                            tempList.put(zip, Integer.parseInt(unit.split("}")[0]));
                        }
                    }
                    i += 1;
                }
            }

            Launcher.alerts = new ArrayList<>();
            for (Map.Entry<String, Integer> element : tempList.entrySet()) {
                Integer prevCount = Launcher.CEPList.get(element.getKey());
                if (prevCount == null) {
                    Launcher.alerts.add(element.getKey());
                }
                else if (element.getValue() > prevCount) {
                    Launcher.alerts.add(element.getKey());
                }
            }
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}

package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
public class BulkRequestContext {
    private List<BulkRequest> requests = new ArrayList<>();

    private boolean odererd = false;

    public void addRequest(BulkRequest br) {
        requests.add(br);
    }

}

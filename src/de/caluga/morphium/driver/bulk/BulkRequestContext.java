package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.driver.MorphiumDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public abstract class BulkRequestContext {
    private List<BulkRequest> requests = new ArrayList<>();

    private boolean odererd = false;

    public void addRequest(BulkRequest br) {
        requests.add(br);
    }

    public abstract Map<String, Object> execute(MorphiumDriver drv);

}

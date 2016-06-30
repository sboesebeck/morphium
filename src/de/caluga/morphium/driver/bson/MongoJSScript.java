package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

import java.util.Map;

/**
 * JSCript implemantation for BSON
 **/
@SuppressWarnings("WeakerAccess")
public class MongoJSScript {
    private String js;
    private Map<String, Object> context;

    public MongoJSScript(String js, Map<String, Object> context) {
        this.js = js;
        this.context = context;
    }

    public MongoJSScript(String js) {
        this.js = js;
    }

    @SuppressWarnings("unused")
    public String getJs() {
        return js;
    }

    @SuppressWarnings("unused")
    public void setJs(String js) {
        this.js = js;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    @SuppressWarnings("unused")
    public void setContext(Map<String, Object> context) {
        this.context = context;
    }
}

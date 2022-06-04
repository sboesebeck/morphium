package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

import de.caluga.morphium.driver.Doc;

import java.util.Map;

/**
 * JSCript implemantation for BSON
 **/
@SuppressWarnings("WeakerAccess")
public class MongoJSScript {
    private String js;
    private Doc context;

    public MongoJSScript(String js, Doc context) {
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

    public Doc getContext() {
        return context;
    }

    @SuppressWarnings("unused")
    public void setContext(Doc context) {
        this.context = context;
    }
}

package de.caluga.morphium.gui.recordedit;


public class RecordModificationException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -7579889523374536122L;
    private String msg;
    private Object r;

    public RecordModificationException(String msg, Object r) {
        super(msg);
        this.msg = msg;
        this.r = r;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getR() {
        return r;
    }

    public void setR(Object r) {
        this.r = r;
    }
}

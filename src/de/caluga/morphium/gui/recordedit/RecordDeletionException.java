package de.caluga.morphium.gui.recordedit;

@SuppressWarnings("ALL")
public class RecordDeletionException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -5301808380031198352L;
    private String msg;
    private Object r;

    public RecordDeletionException(String msg, Object r) {
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

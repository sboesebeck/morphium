package de.caluga.morphium.gui.recordedit;

@SuppressWarnings("SerializableHasSerializationMethods")
public class UpdateException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -3584877267913637899L;

    public UpdateException(String msg) {
        super(msg);
    }
}

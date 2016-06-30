package de.caluga.morphium;

/**
 * will be called by morphium to give control over de-referencing
 * type Parameter:
 * <p/>
 * T: Type of referenced Object
 * E: Type of entity, containing reference
 * I: Type of ID of the referenced Object
 * <p/>
 * User: Stephan Bösebeck
 * Date: 23.07.15
 * Time: 22:03
 * <p/>
 */
public interface DereferencingListener<T, E, I> {

    /**
     * will be called, before a reference is going to be de-referenced
     *
     * @param entityIncludingReference - the Entity which includes the lazy loaded reference
     * @param fieldInEntity            - name of the field containing the reference
     * @param id                       - ID of the referenced Object
     * @param typeReferenced           - type to be used for de-referencing
     * @param lazy                     - true, if lazy loaded reference should be dereferenced
     * @throws MorphiumAccessVetoException - can be thrown, if de-referencing should not take place
     */
    void wouldDereference(E entityIncludingReference, String fieldInEntity, I id, Class<T> typeReferenced, boolean lazy) throws MorphiumAccessVetoException;  //

    /**
     * Will be called, after a lazy loaded reference was de-referenced and unmarshalled
     *
     * @param entitiyIncludingReference - the entity which includes the lazy loaded reference
     * @param fieldInEntity             - the name of the field containing the reference
     * @param referencedObject          - the referenced object, kompletely unmarshalled. This one is actually stored also in the entity (field specified above)
     * @param lazy                      - true, if lazy loaded reference
     * @return
     */
    @SuppressWarnings("UnusedReturnValue")
    T didDereference(E entitiyIncludingReference, String fieldInEntity, T referencedObject, boolean lazy); //returns the object to set as de-referenced object. referencedObject would be null, if not found
}

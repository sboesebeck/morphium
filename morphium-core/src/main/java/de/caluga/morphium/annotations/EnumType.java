package de.caluga.morphium.annotations;

/**
 * How an enum is stored (MOR-093): by the name of its constant, or by its ordinal. The ordinal makes a sort and a range query follow the order of the constants, and changes when a constant is inserted
 * or moved: add new constants at the end.
 *
 * @see Enumerated
 */
public enum EnumType {
    /** the name of the constant, a String: the default */
    STRING,
    /** the position of the constant, an int */
    ORDINAL
}

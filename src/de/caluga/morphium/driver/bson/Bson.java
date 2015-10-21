package de.caluga.morphium.driver.bson;

/**
 * BSon Spec
 * Specification Version 1.0
 * <p/>
 * BSON is a binary format in which zero or more key/id pairs are stored as a single entity. We call this entity a document.
 * <p/>
 * The following grammar specifies version 1.0 of the BSON standard. We've written the grammar using a pseudo-BNF syntax. Valid BSON data is represented by the document non-terminal.
 * <p/>
 * Basic Types
 * <p/>
 * The following basic types are used as terminals in the rest of the grammar. Each type must be serialized in little-endian format.
 * <p/>
 * byte	1 byte (8-bits)
 * int32	4 bytes (32-bit signed integer, two's complement)
 * int64	8 bytes (64-bit signed integer, two's complement)
 * double	8 bytes (64-bit IEEE 754-2008 binary floating point)
 * Non-terminals
 * <p/>
 * The following specifies the rest of the BSON grammar. Note that quoted strings represent terminals, and should be interpreted with C semantics (e.g. "\x01" represents the byte 0000 0001). Also note that we use the * operator as shorthand for repetition (e.g. ("\x01"*2) is "\x01\x01"). When used as a unary operator, * means that the repetition can occur 0 or more times.
 * <p/>
 * document	::=	int32 e_list "\x00"	BSON Document. int32 is the total number of bytes comprising the document.
 * e_list	::=	element e_list
 * |	""
 * element	::=	"\x01" e_name double	64-bit binary floating point
 * |	"\x02" e_name string	UTF-8 string
 * |	"\x03" e_name document	Embedded document
 * |	"\x04" e_name document	Array
 * |	"\x05" e_name binary	Binary data
 * |	"\x06" e_name	Undefined (id) — Deprecated
 * |	"\x07" e_name (byte*12)	ObjectId
 * |	"\x08" e_name "\x00"	Boolean "false"
 * |	"\x08" e_name "\x01"	Boolean "true"
 * |	"\x09" e_name int64	UTC datetime
 * |	"\x0A" e_name	Null id
 * |	"\x0B" e_name cstring cstring	Regular expression - The first cstring is the regex pattern, the second is the regex options string. Options are identified by characters, which must be stored in alphabetical order. Valid options are 'i' for case insensitive matching, 'm' for multiline matching, 'x' for verbose mode, 'l' to make \w, \W, etc. locale dependent, 's' for dotall mode ('.' matches everything), and 'u' to make \w, \W, etc. match unicode.
 * |	"\x0C" e_name string (byte*12)	DBPointer — Deprecated
 * |	"\x0D" e_name string	JavaScript code
 * |	"\x0E" e_name string	Deprecated
 * |	"\x0F" e_name code_w_s	JavaScript code w/ scope
 * |	"\x10" e_name int32	32-bit integer
 * |	"\x11" e_name int64	Timestamp
 * |	"\x12" e_name int64	64-bit integer
 * |	"\xFF" e_name	Min key
 * |	"\x7F" e_name	Max key
 * e_name	::=	cstring	Key name
 * string	::=	int32 (byte*) "\x00"	String - The int32 is the number bytes in the (byte*) + 1 (for the trailing '\x00'). The (byte*) is zero or more UTF-8 encoded characters.
 * cstring	::=	(byte*) "\x00"	Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST NOT contain '\x00', hence it is not full UTF-8.
 * binary	::=	int32 subtype (byte*)	Binary - The int32 is the number of bytes in the (byte*).
 * subtype	::=	"\x00"	Generic binary subtype
 * |	"\x01"	Function
 * |	"\x02"	Binary (Old)
 * |	"\x03"	UUID (Old)
 * |	"\x04"	UUID
 * |	"\x05"	MD5
 * |	"\x80"	User defined
 * code_w_s	::=	int32 string document	Code w/ scope
 */
public interface Bson {

    public enum Type {
        dbl(1), string(2), document(3), array(4), binary(5), undefined(6), objectid(7), bool(8), datetime(9), nullValue(10), regex(11), dbPointer(12), jscript(13), unused(14), jscriptScope(15), int32(16), timestamp(17), int64(18), minKey(255), maxKey(0x7f);

        Type(int id) {
            this.id = id;
        }

        int id;
    }

    Type getType();

    byte[] serialize();

    Bson deserialize(byte[] in);
}

package de.caluga.morphium;


import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>
 * Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
 * The collation document has the following fields:
 * </p>
 * <ul>
 * <li>locale: The locale for the collation. This is a string that represents a specific language and region. For example, "en_US" for American English.</li>
 * <li>caseLevel: A boolean that indicates whether to include case comparison. If true, the comparison considers case.</li>
 * <li>caseFirst: A string that determines the sort order of case. Possible values are "upper", "lower", or "off".</li>
 * <li>strength: The level of comparison to perform. This is an integer from 1 to 5, where 1 is the simplest comparison and 5 is the most complex.</li>
 * <li>numericOrdering: A boolean that determines whether to compare numeric strings as numbers or as strings.</li>
 * <li>alternate: A string that controls the handling of whitespace and punctuation. Possible values are "non-ignorable" and "shifted".</li>
 * <li>maxVariable: A string that determines what characters are considered ignorable when alternate is "shifted". Possible values are "punct" and "space".</li>
 * <li>backwards: A boolean that causes secondary differences to be considered in reverse order, as it is done in the French language.</li>
 * <li>normalization: A boolean that enables or disables the normalization of text for comparison.</li>
 * </ul>
 */
public class Collation {
    private String locale;
    private Boolean caseLevel;
    private CaseFirst caseFirst;
    private Strength strength;
    private Boolean numericOrdering;
    private Alternate alternate;
    private MaxVariable maxVariable;
    private Boolean backwards;
    private Boolean normalization;

    public Collation() {
    }

    public Collation(String locale, Boolean caseLevel, CaseFirst caseFirst, Strength strength, Boolean numericOrdering, Alternate alternate, MaxVariable maxVariable, Boolean backwards, Boolean normalization) {
        this.locale = locale;
        this.caseLevel = caseLevel;
        this.caseFirst = caseFirst;
        this.strength = strength;
        this.numericOrdering = numericOrdering;
        this.alternate = alternate;
        this.maxVariable = maxVariable;
        this.backwards = backwards;
        this.normalization = normalization;
    }

    public Boolean getNormalization() {
        return normalization;
    }

    public Collation normalization(Boolean normalization) {
        this.normalization = normalization;
        return this;
    }

    public String getLocale() {
        return locale;
    }

    public Collation locale(String locale) {
        this.locale = locale;
        return this;
    }

    public Boolean getCaseLevel() {
        return caseLevel;
    }

    public Collation caseLevel(Boolean caseLevel) {
        this.caseLevel = caseLevel;
        return this;
    }

    public CaseFirst getCaseFirst() {
        return caseFirst;
    }

    public Collation caseFirst(CaseFirst caseFirst) {
        this.caseFirst = caseFirst;
        return this;
    }

    public Strength getStrength() {
        return strength;
    }

    public Collation strength(Strength strength) {
        this.strength = strength;
        return this;
    }

    public Boolean getNumericOrdering() {
        return numericOrdering;
    }

    public Collation numericOrdering(Boolean numericOrdering) {
        this.numericOrdering = numericOrdering;
        return this;
    }

    public Alternate getAlternate() {
        return alternate;
    }

    public Collation alternate(Alternate alternate) {
        this.alternate = alternate;
        return this;
    }

    public MaxVariable getMaxVariable() {
        return maxVariable;
    }

    public Collation maxVariable(MaxVariable maxVariable) {
        this.maxVariable = maxVariable;
        return this;
    }

    public Boolean getBackwards() {
        return backwards;
    }

    public Collation backwards(Boolean backwards) {
        this.backwards = backwards;
        return this;
    }

    public Map<String, Object> toQueryObject() {
        Map<String, Object> ret = new LinkedHashMap<>();
        if (locale != null) {
            ret.put("locale", locale);
        }
        if (caseLevel != null) {
            ret.put("caseLevel", caseLevel);
        }
        if (caseFirst != null) {
            ret.put("caseFirst", caseFirst);
        }
        if (strength != null) {
            ret.put("strength", strength);
        }
        if (numericOrdering != null) {
            ret.put("numericOrdering", numericOrdering);
        }
        if (alternate != null) {
            ret.put("alternate", alternate);
        }
        if (maxVariable != null) {
            ret.put("maxVariable", maxVariable);
        }
        if (backwards != null) {
            ret.put("backwards", backwards);
        }
        return ret;
    }

    public enum Alternate {
        SHIFTED("shifted"),
        NON_IGNORABLE("non-ignorable");

        @SuppressWarnings("CanBeFinal")
        String mongoText;

        Alternate(String mongoText) {
            this.mongoText = mongoText;
        }

        public String getMongoText() {
            return mongoText;
        }
    }

    public enum CaseFirst {
        LOWER("lower"),
        OFF("off"),
        UPPER("upper");
        @SuppressWarnings("CanBeFinal")
        String mongoText;

        CaseFirst(String mongoText) {
            this.mongoText = mongoText;
        }

        public String getMongoText() {
            return mongoText;
        }
    }


    public enum MaxVariable {
        PUNCT("punct"),
        SPACE("space");

        @SuppressWarnings("CanBeFinal")
        String mongoText;

        MaxVariable(String mongoText) {
            this.mongoText = mongoText;
        }

        public String getMongoText() {
            return mongoText;
        }
    }

    public enum Strength {
        PRIMARY(1),
        SECONDARY(2),
        TERTIARY(3),
        QUATERNARY(4),
        IDENTICAL(5);


        @SuppressWarnings("CanBeFinal")
        int mongoValue;

        Strength(int mongoValue) {
            this.mongoValue = mongoValue;
        }

        public int getMongoValue() {
            return mongoValue;
        }
    }

}

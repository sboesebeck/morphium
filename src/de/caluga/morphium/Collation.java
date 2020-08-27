package de.caluga.morphium;


import java.util.LinkedHashMap;
import java.util.Map;

/**
 * locale: <string>,
 * caseLevel: <boolean>,
 * caseFirst: <string>,
 * strength: <int>,
 * numericOrdering: <boolean>,
 * alternate: <string>,
 * maxVariable: <string>,
 * backwards: <boolean>
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

    public Collation() {
    }

    public Collation(String locale, Boolean caseLevel, CaseFirst caseFirst, Strength strength, Boolean numericOrdering, Alternate alternate, MaxVariable maxVariable, Boolean backwards) {
        this.locale = locale;
        this.caseLevel = caseLevel;
        this.caseFirst = caseFirst;
        this.strength = strength;
        this.numericOrdering = numericOrdering;
        this.alternate = alternate;
        this.maxVariable = maxVariable;
        this.backwards = backwards;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public Boolean getCaseLevel() {
        return caseLevel;
    }

    public void setCaseLevel(Boolean caseLevel) {
        this.caseLevel = caseLevel;
    }

    public CaseFirst getCaseFirst() {
        return caseFirst;
    }

    public void setCaseFirst(CaseFirst caseFirst) {
        this.caseFirst = caseFirst;
    }

    public Strength getStrength() {
        return strength;
    }

    public void setStrength(Strength strength) {
        this.strength = strength;
    }

    public Boolean getNumericOrdering() {
        return numericOrdering;
    }

    public void setNumericOrdering(Boolean numericOrdering) {
        this.numericOrdering = numericOrdering;
    }

    public Alternate getAlternate() {
        return alternate;
    }

    public void setAlternate(Alternate alternate) {
        this.alternate = alternate;
    }

    public MaxVariable getMaxVariable() {
        return maxVariable;
    }

    public void setMaxVariable(MaxVariable maxVariable) {
        this.maxVariable = maxVariable;
    }

    public Boolean getBackwards() {
        return backwards;
    }

    public void setBackwards(Boolean backwards) {
        this.backwards = backwards;
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


        int mongoValue;

        Strength(int mongoValue) {
            this.mongoValue = mongoValue;
        }

        public int getMongoValue() {
            return mongoValue;
        }
    }

}

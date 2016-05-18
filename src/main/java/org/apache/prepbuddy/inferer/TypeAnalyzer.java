package org.apache.prepbuddy.inferer;

import org.apache.prepbuddy.filetypes.Type;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.prepbuddy.filetypes.Type.*;

public class TypeAnalyzer implements Serializable {
    private List<String> sampleData;

    public TypeAnalyzer(List<String> sampleData) {
        this.sampleData = sampleData;
    }

    public Type getType() {
        Type type = getBaseType();
        if (type.equals(NUMERIC)){
            if (isDecimal())
                return DECIMAL;
            if (isInt())
                return INT;
        }
        if (type.equals(STRING)){
            if(isEmail()) {
                return EMAIL;
            }
        }
        return type;
    }

    private boolean isDate() {
        return matchesWith("");
    }

    private boolean isInt() {
        return matchesWith("\\d+");
    }

    private boolean isDecimal() {
        return matchesWith("\\.\\d+|\\d+\\.\\d+");
    }

    private boolean matchesWith(String regex) {
        ArrayList<Boolean> booleen = new ArrayList<>();
        for (String string : sampleData)
            if (string.matches(regex)) booleen.add(true);
            else booleen.add(false);
        return !booleen.contains(false);
    }

    private Type getBaseType() {
        if (matchesWith("\\d+(\\.\\d+|\\d+)|\\.\\d+"))
            return NUMERIC;
        return STRING;
    }
    public boolean isEmail() {
        return matchesWith("^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$");
    }
}

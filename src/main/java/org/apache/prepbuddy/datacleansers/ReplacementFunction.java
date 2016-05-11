package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.utils.Replacement;

import java.io.Serializable;

public class ReplacementFunction implements Serializable {
    private Replacement[] replacements;

    public ReplacementFunction(Replacement... replacements) {
        this.replacements = replacements;
    }

    public String replace(String current) {
        for (Replacement replacement : replacements) {
            if (replacement.matches(current)) return replacement.replacementValue();
        }
        return current;
    }
}

package org.apache.prepbuddy.reformatters;

class NominalToNumericTransformation implements TransformerFunction {
    private final Replacement[] pairs;
    private final DefaultValue defaultt;

    public NominalToNumericTransformation(DefaultValue defaultt,Replacement... pairs) {
        this.pairs = pairs;
        this.defaultt = defaultt;
    }

    @Override
    public String apply(String existing) {
        for (Replacement pair : pairs) {
            if (pair.matches(existing)) {
                return pair.replacementValue();
            }
        }
        return defaultt.asString();
    }
}

package org.apache.prepbuddy.reformatters;

class NominalToNumericTransformation implements TransformationFunction {
    private final Replacement[] pairs;
    private final DefaultValue defaultt;

    public NominalToNumericTransformation(DefaultValue defaultt,Replacement... pairs) {
        this.pairs = pairs;
        this.defaultt = defaultt;
    }

    @Override
    public String apply(String existingValue, String[] row) {
        for (Replacement pair : pairs) {
            if (pair.matches(existingValue)) {
                return pair.replacementValue();
            }
        }
        return defaultt.asString();
    }
}

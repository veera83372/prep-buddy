package org.apache.prepbuddy.datacleansers.imputation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class LikelihoodTableTest {

    @Test
    public void shouldAllowLookingUpProbabilities() throws Exception {
        LikelihoodTable table = new LikelihoodTable();
        List<String> strings = Arrays.asList(new String[]{"buy_yes", "buy_no"});
        table.addRowKeys(strings);
        table.setProbability("buy_yes", "age_less_than_30", Probability.create(0.5));
        Probability prob = table.lookup("buy_yes", "age_less_than_30");
        assertEquals(Probability.create(0.5), prob);
    }
}
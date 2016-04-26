package main.java.org.apache.prepbuddy.transformations;

import org.apache.prepbuddy.transformations.deduplication.DeduplicationInput;
import org.apache.prepbuddy.transformations.deduplication.DeduplicationTransformation;
import org.junit.Test;

public class DeduplicationTransformationTest {

    @Test
    public void shouldDeduplicateRecordsBasedOnSelectedColumns() {
        DeduplicationTransformation deDupeTransform = new DeduplicationTransformation("data/calls.csv","data/deduplicated_calls" );
        deDupeTransform.apply(new DeduplicationInput());
    }
}

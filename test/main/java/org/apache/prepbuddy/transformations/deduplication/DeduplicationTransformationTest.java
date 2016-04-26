package org.apache.prepbuddy.transformations.deduplication;

import org.junit.Test;

public class DeduplicationTransformationTest {

    @Test
    public void shouldDeduplicateRecordsBasedOnSelectedColumns() {
        DeduplicationTransformation deDupeTransform = new org.apache.prepbuddy.transformations.deduplication.DeduplicationTransformation("data/calls.csv","data/deduplicated_calls" );
        deDupeTransform.apply(new org.apache.prepbuddy.transformations.deduplication.DeduplicationInput());
    }
}

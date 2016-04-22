package org.apache.prepbuddy.transformations;

import org.apache.prepbuddy.transformations.deduplication.DeduplicationInput;

public interface DataTransformation {
    void apply(DeduplicationInput input);
}

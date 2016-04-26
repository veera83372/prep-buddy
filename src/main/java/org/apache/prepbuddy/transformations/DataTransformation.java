package org.apache.prepbuddy.transformations;

import org.apache.prepbuddy.transformations.deduplication.DeduplicationConfig;

public interface DataTransformation {
    void apply(DeduplicationConfig input);
}

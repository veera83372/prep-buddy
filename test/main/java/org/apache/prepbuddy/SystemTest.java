package org.apache.prepbuddy;

import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.datacleansers.ReplacementFunction;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformation.ColumnJoiner;
import org.apache.prepbuddy.transformation.MarkerPredicate;
import org.apache.prepbuddy.transformation.SplitByDelimiter;
import org.apache.prepbuddy.transformation.SplitByFieldLength;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static junit.framework.Assert.assertEquals;

public class SystemTest extends SparkTestCase {

    @Test
    public void shouldExecuteASeriesOfTransformsOnADataset() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicated = initialRDD.deduplicate();
        assertEquals(2, deduplicated.count());

        TransformableRDD purged = deduplicated.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.get(1).equals("YY");
            }
        });
        assertEquals(1, purged.count());

         TransformableRDD marked = purged.flag("*", new MarkerPredicate() {
             @Override
             public boolean evaluate(RowRecord row) {
                 return true;
             }
         });

        assertEquals(1, marked.count());
        assertEquals("X,Y,,*", marked.first());

        TransformableRDD mapedRDD = marked.mapByFlag("*",3, new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                return "Star " + row;
            }
        });

        assertEquals(1, mapedRDD.count());
        assertEquals("Star X,Y,,*", mapedRDD.first());

        TransformableRDD unflaged = mapedRDD.removeFlag(3);

        assertEquals("Star X,Y,", unflaged.first());
        TransformableRDD imputedRDD = purged.impute(2, new MissingDataHandler() {
            @Override
            public String handleMissingData(RowRecord record) {
                return "Male";
            }
        });
        assertEquals("X,Y,Male", imputedRDD.first());

        TransformableRDD numericRDD = imputedRDD.replace(2, new ReplacementFunction(new Replacement<>("Male", 0),
                new Replacement<>("Female", 1)));

        assertEquals(1, numericRDD.count());
        assertEquals("X,Y,0", numericRDD.first());
    }

    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        TransformableRDD rdd = new TransformableRDD(initialDataset);
        TextFacets facets = rdd.listFacets(0);
        assertEquals(2, facets.count());
    }

    @Test
    public void shouldBeAbleToSplitTheGivenColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Collections.singletonList("FirstName LastName MiddleName,850"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD splitColumnRDD = initialRDD.splitColumn(0, new SplitByDelimiter(" ", false));
        assertEquals("FirstName,LastName,MiddleName,850", splitColumnRDD.first());

        TransformableRDD splitColumnRDDByKeepingColumn = initialRDD.splitColumn(0, new SplitByDelimiter(" ", true));
        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", splitColumnRDDByKeepingColumn.first());

        TransformableRDD splitColumnByLengthRDD = initialRDD.splitColumn(0, new SplitByFieldLength(Arrays.asList(9, 9), false));
        assertEquals("FirstName, LastName,850", splitColumnByLengthRDD.first());

        TransformableRDD splitColumnByLengthRDDByKeepingColumn = initialRDD.splitColumn(0, new SplitByFieldLength(Arrays.asList(9, 9), true));
        assertEquals("FirstName LastName MiddleName,FirstName, LastName,850", splitColumnByLengthRDDByKeepingColumn.first());
    }

    @Test
    public void shouldBeAbleToJoinMultipleColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Collections.singletonList("FirstName,LastName,732,MiddleName"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD joinedColumnRDD = initialRDD.joinColumns(new ColumnJoiner(Arrays.asList(3, 1, 0), "_", false));
        assertEquals("732,MiddleName_LastName_FirstName", joinedColumnRDD.first());

        TransformableRDD joinedColumnRDDByKeepingOriginals = initialRDD.joinColumns(new ColumnJoiner(Arrays.asList(3, 1, 0), "_", true));
        assertEquals("FirstName,LastName,732,MiddleName,MiddleName_LastName_FirstName", joinedColumnRDDByKeepingOriginals.first());

        TransformableRDD joinedColumnWithDefault = initialRDD.joinColumns(new ColumnJoiner(Arrays.asList(3, 1, 0), false));
        assertEquals("732,MiddleName LastName FirstName", joinedColumnWithDefault.first());
    }
}

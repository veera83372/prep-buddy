package org.apache.prepbuddy.transformations.imputation;

//public class ImputationTest extends SparkTestCase {
//    private ImputationTransformation imputation;
//
//    @Before
//    public void setUp() throws Exception {
//        super.setUp();
//        imputation = new ImputationTransformation();
//        getLogger("org").setLevel(Level.OFF);
//    }
//
//    @Test
//    public void shouldCallbackTheMissingDataHandler() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList(",,4,5"));
//        Imputers imputers = new Imputers();
//        imputers.add(0, new Imputers.HandlerFunction() {
//            @Override
//            public String handleMissingField(RowRecord rowRecord) {
//                return "1234567890";
//            }
//        });
//        imputers.add(1, new Imputers.HandlerFunction() {
//            @Override
//            public String handleMissingField(RowRecord rowRecord) {
//                return "000000";
//            }
//        });
//
//        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers, FileType.CSV);
//
//        String expected = "1234567890,000000,4,5";
//        String actual = transformed.first();
//        Assert.assertEquals(expected, actual);
//    }
//
//    @Test(expected = SparkException.class)
//    public void shouldThrowExceptionIfIndexIsInvalid() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,,4,5"));
//        Imputers imputers = new Imputers();
//        imputers.add(6, new Imputers.HandlerFunction() {
//            @Override
//            public String handleMissingField(RowRecord rowRecord) {
//                return "1";
//            }
//
//        });
//
//        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers, FileType.CSV);
//        String expected = "1,1,4,5";
//        String actual = transformed.first();
//        Assert.assertEquals(expected, actual);
//    }
//
//    @Test
//    public void shouldRemoveTheEntireRowWhenDataIsMissing() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,,4,5", "3,5,6"));
//        Remover remover = new Remover();
//        remover.onColumn(0);
//        remover.onColumn(1);
//
//        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover, FileType.CSV);
//
//        String expected = "3,5,6";
//        String actual = transformed.first();
//        Assert.assertEquals(expected, actual);
//    }
//
//
//    @Test(expected = SparkException.class)
//    public void shouldThrowExceptionWhenInvalidColumnIndexIsGivenToRemover() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,,4,5", "3,5,6"));
//        Remover remover = new Remover();
//        remover.onColumn(10);
//        remover.onColumn(1);
//
//        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover, FileType.CSV);
//        transformed.first();
//    }
//
//    @Test(expected = SparkException.class)
//    public void shouldThrowExceptionWhenLessThenZeroColumnIndexIsGivenToRemover() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,,4,5", "3,5,6"));
//        Remover remover = new Remover();
//        remover.onColumn(-1);
//        remover.onColumn(1);
//
//        imputation.removeIfNull(initialDataset, remover, FileType.CSV).first();
//    }
//
//    @Test
//    public void shouldCallbackForTSVData() {
//        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("2\t \t5"));
//        Imputers imputers = new Imputers();
//        imputers.add(1, new Imputers.HandlerFunction() {
//            @Override
//            public String handleMissingField(RowRecord rowRecord) {
//                return rowRecord.get(0);
//            }
//        });
//
//        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers, FileType.TSV);
//
//        String expected = "2\t2\t5";
//        String actual = transformed.first();
//        Assert.assertEquals(expected, actual);
//    }
//}

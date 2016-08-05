
var appendAll = function() {
	appendHeading("Deduplicate()", "h4");
	var aboutDeduplicate = 'It gives a new JavaTransformableRDD with unique values by eliminating the duplicate records.';
	appendParagraph(aboutDeduplicate);
	var deduplicateJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
                'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
                'JavaTransformableRDD deduplicatedRDD = initialRDD.deduplicate();',
                'deduplicatedRDD.saveAsTextFile("output");' ];
    var deduplicateScalaCode = ['callDataset: RDD[String] = sc.textFile("calls.csv")',
                'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
                'deduplicatedRDD: TransformableRDD = initialRDD.deduplicate()',
                'deduplicatedRDD.saveAsTextFile("output")' ];            
    appendCode(deduplicateScalaCode, deduplicateJavaCode);            
	var deduplicateExplanation = 'In the sample dataset given above, there are 2 records with repetition. So, after calling the deduplicate method, the deduplicatedRDD will hold the following values:'
	appendParagraph(deduplicateExplanation);
	
	var deduplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010']
	appendExample(deduplicateOutput);

	appendHeading('duplicates()', 'h4');
	var aboutDuplicates = 'It gives a new JavaTransformableRDD with only the records which has a duplicate entry in the given rdd';
	appendParagraph(aboutDuplicates);

	appendHeading('Example:', 'h4');
	var duplicateJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
						'JavaTransformableRDD duplicates = initialRDD.duplicates();',
						'duplicates.saveAsTextFile("output");']
	var duplicateScalaCode = ['callDataset: RDD[String] = sc.textFile("calls.csv")',
						'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
						'duplicates: TransformableRDD = initialRDD.duplicates()',
						'duplicates.saveAsTextFile("output")']					
	appendCode(duplicateScalaCode, duplicateJavaCode);					
	appendParagraph('It will produce the following result:');

	var duplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011'];
	appendExample(duplicateOutput);

	// appendHeading('splitColumn(int columnIndex, ColumnSplitter columnSplitConfig)', 'h4');
	// appendParagraph('It splits a particular column by a given configuration. There are two types of column splitting operation available');
	// appendHeading('SplitByDelimiter');
	// appendParagraph(' takes a string delimiter and a boolean value flagging whether to retain the original column or not.');

	appendHeading('listFacets(int columnIndex):','h4');
	appendParagraph('In dataset we want to know the number of occurrences for each value of any field.');
	appendParagraph('Let say we want facets on direction column. So we just need to pass the column index of direction field to listFacets. It returns a TextFacets object.');
	var facetJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
						'TextFacets facets = initialRDD.listFacets(2);',
						'System.out.println(facets.highest());']
	var facetScalaCode = ['callDataset: RDD[String] = sc.textFile("calls.csv")',
						'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
						'facets: TextFacets = initialRDD.listFacets(2)',
						'println(facets.highest())']
	appendCode(facetScalaCode, facetJavaCode);
	appendParagraph('Output of the above code is:');
	appendExample(['(Missed, 4)']);

	appendHeading('flag(String symbol, new MarkerPredicate):', 'h4');
	appendParagraph('Flag is useful when we want mark rows as a favorite or important row.So that we can perform some operation on those rows');
	appendParagraph('Let say we want to mark those row on sample dataset whose field "direction" is "Outgoing" by "#" symbol.  Here is the code for it:');
	var flagJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
					'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
					'JavaTransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;@Override',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;public boolean evaluate(RowRecord row) {',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String direction = row.select(2);',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return direction.equals("Outgoing");',
					'&nbsp;&nbsp;&nbsp;&nbsp;}',	
					'});',	
					'marked.saveAsTextFile("flagged-data");'];
	var flagScalaCode = ['callDataset RDD[String] = sc.textFile("calls.csv")',
					'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
					'marked: TransformableRDD = initialRDD.flag("#", (rowRecord) => {',
					'&nbsp;&nbsp;&nbsp;&nbsp; direction: String = row.select(2)',
					'&nbsp;&nbsp;&nbsp;&nbsp; direction.equals("Outgoing")',
					'});',	
					'marked.saveAsTextFile("flagged-data");'];				
	appendCode(flagScalaCode, flagJavaCode);
	appendParagraph('It will save the file as "flagged-data" with the following records:');
	var flagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
						'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
						'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
						'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,',
						'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,'
						];
	appendExample(flagOutput);

	appendHeading('mapByFlag(String symbol, int symbolColumnIndex, Function mapFunction) :', 'h4');
	appendParagraph('We want map only on marked rows ');
	var mapByFlagJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
						'JavaTransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic boolean evaluate(RowRecord row) {',
							'&nbsp&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspString direction = row.select(2);',
							'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspreturn direction.equals("Outgoing");',
						'}',	
						'});',
						'JavaTransformableRDD mapped = marked.mapByFlag("#", 5, new Function<String, String>(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic String call(String row) throws Exception {',
								'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspReturn "+" + row;',
						'&nbsp;&nbsp;&nbsp;&nbsp;}',
						'});',
						'mapped.saveAsTextFile("marked-data");'
						];
	var mapByFlagScalaCode = ['callDataset: RDD[String] = sc.textFile("calls.csv")',
						'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
						'marked: TransformableRDD = initialRDD.flag("#", (rowRecord) => {',
						'&nbsp;&nbsp;&nbsp;&nbsp; direction: String = row.select(2)',
						'&nbsp;&nbsp;&nbsp;&nbsp; direction.equals("Outgoing")',
						'});',
						'mapped: TransformableRDD = marked.mapByFlag("#", 5, (row) => "+" + row)',
						'mapped.saveAsTextFile("marked-data")'
						];						
	appendCode(mapByFlagScalaCode, mapByFlagJavaCode);
	appendParagraph('The above code will write file which will be look like:');
	var mapByFlagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
							'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,'
							];
	appendExample(mapByFlagOutput);

	appendHeading('removeRows(RowPurger.Predicate predicate):', 'h4');
	appendParagraph('It is useful when we want to remove rows from dataset for a given condition.');
	appendParagraph('Let say we want to remove those row whose field “direction” is Missed.Here is the code:');

	var removeRowJavaCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
						'JavaTransformableRDD purged =  initialRDD.removeRows(new RowPurger.Predicate(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic Boolean evaluate(RowRecord row) {',
							'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspreturn row.select(2).equals("Missed");',
						'&nbsp;&nbsp;&nbsp;&nbsp;}',
						'});',
						'purged.saveAsTextFile("output");'
						];
	var removeRowScalaCode = ['callDataset: RDD[String] = sc.textFile("calls.csv")',
						'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
						'purged: TransformableRDD = initialRDD.removeRows((rowRecord) => row.select(2).equals("Missed"))',
						'purged.saveAsTextFile("output");'
						];						
	appendCode(removeRowScalaCode, removeRowJavaCode);
	appendParagraph('In above code will write file which will be look like:');
	var removeRowOutput = ['07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010'
							];
	appendExample(removeRowOutput);

	appendHeading('clusters(int columnIndex, ClusteringAlgorithm algorithm)');
	appendParagraph('In this method we have to pass the clustering algorithm by which we want to group the similar item in given column index.');
	appendParagraph('We are introducing three algorithm for clustering');
	var imputationList = ['Simple Fingerprint algorithm',
							'N-Gram Fingerprint algorithm',
							'Levenshtein distance algorithm'
							];
	appendList(imputationList);
	appendHeading('By Simple fingerprint algorithm:', 'h4');
	appendCode(['clusters: Clusters = transformedRDD.clusters(2 ,new SimpleFingerprintAlgorithm())'], ['Clusters clusters = transformedRDD.clusters(2 ,new SimpleFingerprintAlgorithm());']);
	appendHeading('By N-Gram Fingerprint algorithm :', 'h4');
	appendParagraph('In this algorithm we pass the value of n which is the size of chars of the token.')
	appendCode(['clusters: Clusters = transformedRDD.clusters(2 ,new NGramFingerprintAlgorithm(2))'], ['Clusters clusters = transformedRDD.clusters(2 ,new NGramFingerprintAlgorithm(2));']);
	appendHeading('By Levenshtein distance algorithm:', 'h4');
	appendParagraph('This algorithm groups the item of field if distance between them is very less.  ');
	appendCode(['clusters: Clusters = transformedRDD.clusters(2 ,new LevenshteinDistance())'], ['Clusters clusters = transformedRDD.clusters(2 ,new LevenshteinDistance());']);
	
	appendHeading('impute(int columnIndex,  ImputationStrategy strategy)', 'h4');
	appendParagraph('It takes column index and a stategy ImputationStrategy. This method replaces the missing value with the value returned by the strategy.');
	appendParagraph('We have a sample dataset missingCalls.csv: ');
	var imputationDataset = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
				'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
				'07122915122,07220374233,Missed,24,Sun Oct 24 08:13:45 +0100 2010',
				'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
				'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
				'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
				'07456622368,07331532487, Missed,  ,Sat Sep 18 13:34:09 +0100 2010'
					];
	appendExample(imputationDataset);
	appendParagraph('In this sample data, we want to impute at duration field.');
	appendHeading('Imputation by mean:', 'h3');
	var imputeByMeanJavaCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
							'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
							'JavaTransformableRDD imputedRDD = initialRDD.impute(3, new MeanStrategy());',
							'ImputedRDD.saveAsTextFile("output");'
								];
	var imputeByMeanScalaCode = ['callDataset: RDD[String] = sc.textFile("missingCalls.csv")',
							'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
							'imputedRDD: TransformableRDD = initialRDD.impute(3, new MeanStrategy())',
							'ImputedRDD.saveAsTextFile("output")'];								
	appendCode(imputeByMeanScalaCode, imputeByMeanJavaCode);
	appendHeading('output:', 'h3');
	var imputeByMeanOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
							'07456622368,07331532487, Missed,<spam class="light">16.57</spam>,Sat Sep 18 13:34:09 +0100 2010'
								];
	appendExample(imputeByMeanOutput);

	appendHeading('Impute by approx mean :', 'h4');
	var imputeByApproxMeanJavaCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
									'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
									'JavaTransformableRDD imputedRDD = initialRDD.impute(3, new ApproxMeanStrategy());',
									'ImputedRDD.saveAsTextFile("output");'
									];
	var imputeByApproxMeanScalaCode = ['callDataset: RDD[String] = sc.textFile("missingCalls.csv")',
									'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
									'imputedRDD: TransformableRDD = initialRDD.impute(3, new ApproxMeanStrategy())',
									'ImputedRDD.saveAsTextFile("output")'];								
	appendCode(imputeByApproxMeanScalaCode, imputeByApproxMeanJavaCode);
	
	appendHeading('output:', 'h4');
	var imputeByApproxMeanOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
									'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
									'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
									'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
									'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
									'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
									'07456622368,07331532487, Missed,<spam class="light">16.57</spam>,Sat Sep 18 13:34:09 +0100 2010'
										];
	appendExample(imputeByApproxMeanOutput);									
	appendHeading('Impute by mode:', 'h3');
	var imputeByModeJavaCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
							'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
							'JavaTransformableRDD imputedRDD = initialRDD.impute(3, new ModeSubstitution());',
							'ImputedRDD.saveAsTextFile("output");'
							];
	var imputeByModeScalaCode = ['callDataset: JavaRDD[String] = sc.textFile("missingCalls.csv")',
							'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
							'imputedRDD: TransformableRDD = initialRDD.impute(3, new ModeSubstitution())',
							'ImputedRDD.saveAsTextFile("output")'];							
	appendCode(imputeByModeScalaCode, imputeByModeJavaCode);						
	appendHeading('output:', 'h3');
	var imputeByModeOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
								'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
								'07122915122,07220374233,Missed,24,Sun Oct 24 08:13:45 +0100 2010',
								'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
								'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
								'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
								'07456622368,07331532487, Missed,<spam class="light">24</spam>,Sat Sep 18 13:34:09 +0100 2010'
								];
	appendExample(imputeByModeOutput);	

	appendHeading('Impute by naive bayes classifier:', 'h3');
	appendParagraph("This imputation is helpful when we want to predict the categorical field's missing value");
	appendParagraph('We have dataset called people.csv');
	var naiveBayesDataset = ['Name,Over 170CM, Eye, Hair length, Sex',
								'Drew,No,Blue,Short,Male', 
								'Claudia,Yes,Brown,Long,Female', 
								'Drew,No,Blue,Long,Female', 
								'Drew,No,Blue,Long,Female',
								'Alberto,Yes,Brown,Short,Male',
								'<spam style="color:rebeccapurple">Drew,Yes,Blue,Long,</spam>',
								'Karin,No,Blue,Long,Female', 
								'Nina,Yes,Brown,Short,Female', 
								'Sergio,Yes,Blue,Long,Male'
								];
	appendExample(naiveBayesDataset);
	appendParagraph('We want to predict the missing value of sex field.');
	var naiveBayesJavaCode = ['JavaRDD<String> callDataset= sc.textFile("people.csv");',
							'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
							'JavaTransformableRDD imputedRDD = initialRDD.impute(4, new NaiveBayesSubstitution(new int[]{0,1,2,3}));',
							'ImputedRDD.saveAsTextFile("output");'
							];
	var naiveBayesScalaCode = ['callDataset: RDD[String] = sc.textFile("people.csv")',
							'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
							'imputedRDD: TransformableRDD = initialRDD.impute(4, new NaiveBayesSubstitution(Array(0,1,2,3,4)))',
							'ImputedRDD.saveAsTextFile("output")'
							];							
	appendCode(naiveBayesScalaCode, naiveBayesJavaCode);
	appendHeading('Output:', 'h3');
	var naiveBayesOutput = ['Name,Over 170CM, Eye, Hair length, Sex',
							'Drew,No,Blue,Short,Male', 
							'Claudia,Yes,Brown,Long,Female', 
							'Drew,No,Blue,Long,Female', 
							'Drew,No,Blue,Long,Female',
							'Alberto,Yes,Brown,Short,Male',
							'Drew,Yes,Blue,Long,Female',
							'Karin,No,Blue,Long,Female',
							'Nina,Yes,Brown,Short,Female', 
							'Sergio,Yes,Blue,Long,Male'
								];
	appendExample(naiveBayesOutput);

	// appendHeading('Type Inference:', 'h3');
	// appendParagraph('When you don’t know the type of a particular column you can infer the dataType of it.It has varitey of type which are  useful.');
	// appendHeading('Example:', 'h3');
	// var typeInferCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
	// 					'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
	// 					'DataType datatype = initialRDD.inferType(1);',
	// 					'System.out.println(datatype);'
	// 						];
	// appendCode([], typeInferCode)
	// appendHeading('Output:', 'h3');
	// appendExample(['MOBILE_NUMBER']);						

	appendHeading('smooth(int columnIndex, SmoothingMethod smoothingMethod):', 'h3');
	appendParagraph('Smoothing is very popular in data analysis by being able to extract more information from the dataset.');
	appendParagraph('We are introducing two moving average methods for smoothing:');
	
	appendParagraph('We have sales dataset called sales.csv');
	var simpleSmoothingDataset = ['Year,Sale','2002,4',
									'2003,6',
									'2004,5', 
									'2005,3',
									'2006,7',
									'2007,5'
								];
	appendExample(simpleSmoothingDataset);
	appendHeading('Smoothing By Simple Moving Average:', 'h3');
	appendParagraph('To smooth data by Simple Moving Average we need to specify the window size to the constructor');
	var simpleSmoothingJavaCode = ['JavaRDD<String> callDataset= sc.textFile("sales.csv");',
								'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
								'JavaDoubleRDD smoothed = initialRDD.smooth(0, new SimpleMovingAverageMethod(3));',
								'smoothed.saveAsTextFile("smoothed");'
								];
	var simpleSmoothingScalaCode = ['callDataset: RDD[String] = sc.textFile("sales.csv")',
								'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
								'smoothed: RDD[Double] = initialRDD.smooth(0, new SimpleMovingAverageMethod(3))',
								'smoothed.saveAsTextFile("smoothed")'
								];								
	appendCode(simpleSmoothingScalaCode, simpleSmoothingJavaCode);
	appendHeading('Output:', 'h3');
	var simpleSmoothingOutput = ['5.0',
								'4.666',
								'5.0',
								'5.0'
									];
	appendExample(simpleSmoothingOutput);

	appendHeading('By Weighted Moving Average:', 'h3');
	appendParagraph('To smooth data by this method we need to pass  Weights to the constructor which contains the weight values according to the window position.');
	appendParagraph('Note: Sum of the weights should be up to one.');
	var weightedJavaCode = ['JavaRDD<String> callDataset= sc.textFile("sales.csv");',
						'JavaTransformableRDD initialRDD = new JavaTransformableRDD(callDataset);',
						'Weights weights = new Weights(3);',
						'weights.add(0.166);',
						'weights.add(0.333);',
						'weights.add(0.5);',
						'JavaDoubleRDD smoothed = initialRDD.smooth(0, new WeightedMovingAverageMethod(3, weights));',
						'smoothed.saveAsTextFile(“smoothed”);'
						];
	var weightedScalaCode = ['callDataset: RDD[String] = sc.textFile("sales.csv")',
						'initialRDD: TransformableRDD = new TransformableRDD(callDataset)',
						'weights: Weights = new Weights(3)',
						'weights.add(0.166)',
						'weights.add(0.333)',
						'weights.add(0.5)',
						'smoothed: RDD[Double] = initialRDD.smooth(1, new WeightedMovingAverageMethod(3, weights))',
						'smoothed.saveAsTextFile(“smoothed”)'
						];						
	appendCode(weightedScalaCode, weightedJavaCode);
	appendHeading('Output:', 'h3');
	var weightedOutput = ['5.162',
						'5.998',
						'5.329',
						'6.999'
							];
	appendExample(weightedOutput);		

	$(".scala").click(showScalaCode)
	$(".java").click(showJavaCode)			
}

var showScalaCode = function() {
	$(".java").removeClass("btnClicked")
	$(".scala").addClass("btnClicked")
	$(".ScalaDoc").removeClass("zero hidden")
	$(".JavaDoc").addClass("zero hidden")
}

var showJavaCode = function() {
	$(".scala").removeClass("btnClicked")
	$(".java").addClass("btnClicked")
	$(".JavaDoc").removeClass("zero hidden")
	$(".ScalaDoc").addClass("zero hidden")
}

$(document).ready(appendAll)
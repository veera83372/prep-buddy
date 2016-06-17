
var appendAll = function() {
	var deduplicateExplanation = 'In the sample dataset given above, there are 2 records with repetition. So, after calling the deduplicate method, the deduplicatedRDD will hold the following values:'
	appendParagraph(deduplicateExplanation);
	
	var deduplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010\n',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010\n',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011\n',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010\n',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010\n']
	appendCode(deduplicateOutput);

	appendHeading('duplicates()', 'h4');
	var aboutDuplicates = 'It gives a new TransformableRDD with only the records which has a duplicate entry in the given rdd';
	appendParagraph(aboutDuplicates);

	appendHeading('Example:', 'h4');
	var duplicateCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
						'TransformableRDD duplicates = initialRDD.duplicates();\n',
						'duplicates.saveAsTextFile("output");']
	appendCode(duplicateCode);					
	appendParagraph('It will produce the following result:');

	var duplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010\n',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011'];
	appendCode(duplicateOutput);
	
	appendHeading('replace(int columnIndex, Replacemet...replacement) :', 'h4');
	var aboutReplace = 'The replace function is used when you want to replace a particular column value to a new value. You have to pass the column Index along with the one or more replacement.';
	appendParagraph(aboutReplace);
	appendHeading('Example:', 'h4');
	var replaceCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
						'TransformableRDD transformedRDD = initialRDD.replace(2, new Replacement("Missed", 0));\n',
						'transformedRDD.saveAsTextFile("output");'];
	appendCode(replaceCode)
	appendHeading('Output:', 'h4');
	var replaceOutput = ['07681546436,07289049655,0,11,Sat Sep 18 01:54:03 +0100 2010\n',
						'07681546436,07289049655,0,11,Sat Sep 18 01:54:03 +0100 2010\n',
						'07122915122,07220374233,0,0,Sun Oct 24 08:13:45 +0100 2010\n',
						'07166594208,07577423566,0,24,Thu Jan 27 14:23:39 +0000 2011\n',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011\n',
						'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010\n',
						'07456622368,07331532487,0,24,Sat Sep 18 13:34:09 +0100 2010'];
	appendCode(replaceOutput);

	// appendHeading('splitColumn(int columnIndex, ColumnSplitter columnSplitConfig)', 'h4');
	// appendParagraph('It splits a particular column by a given configuration. There are two types of column splitting operation available');
	// appendHeading('SplitByDelimiter');
	// appendParagraph(' takes a string delimiter and a boolean value flagging whether to retain the original column or not.');

	appendHeading('listFacets(int columnIndex):','h4');
	appendParagraph('In dataset we want to know the number of occurrences for each value of any field.');
	appendParagraph('Let say we want facets on direction column. So we just need to pass the column index of direction field to listFacets. It returns a TextFacets object.');
	var facetCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
						'TextFacets facets = initialRDD.listFacets(2);\n',
						'System.out.println(facets.highest());']
	appendCode(facetCode);
	appendParagraph('Output of the above code is:');
	appendCode(['(Missed, 4)']);

	appendHeading('flag(String symbol, new MarkerPredicate):', 'h4');
	appendParagraph('Flag is useful when we want mark rows as a favorite or important row.So that we can perform some operation on those rows');
	appendParagraph('Let say we want to mark those row on sample dataset whose field "direction" is "Outgoing" by "#" symbol.  Here is the code for it:');
	var flagCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
					'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
					'TransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;@Override\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;public boolean evaluate(RowRecord row) {\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String direction = row.get(2);\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return direction.equals("Outgoing");\n',
					'&nbsp;&nbsp;&nbsp;&nbsp;}\n',	
					'});\n',	
					'marked.saveAsTextFile("flagged-data");'];
	appendCode(flagCode);
	appendParagraph('It will save the file as "flagged-data" with the following records:');
	var flagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,\n',
						'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,\n',
						'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,\n',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#\n',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#\n',
						'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,\n',
						'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,\n'
						];
	appendCode(flagOutput);

	appendHeading('mapByFlag(String symbol, int symbolColumnIndex, Function mapFunction) :', 'h4');
	appendParagraph('We want map only on marked rows ');
	var mapByFlagCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
						'TransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){\n',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override\n',
							'&nbsp;&nbsp;&nbsp;&nbsppublic boolean evaluate(RowRecord row) {\n',
							'&nbsp&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspString direction = row.get(2);\n',
							'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspreturn direction.equals("Outgoing");',
						'}\n',	
						'});\n',
						'TransformableRDD mapped = marked.mapByFlag("#", 5, new Function<String, String>(){\n',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override\n',
							'&nbsp;&nbsp;&nbsp;&nbsppublic String call(String row) throws Exception {\n',
								'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspReturn "+" + row;\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;}\n',
						'});\n',
						'mapped.saveAsTextFile("marked-data");\n'
						];
	appendCode(mapByFlagCode);
	appendParagraph('The above code will write file which will be look like:');
	var mapByFlagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,\n',
							'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,\n',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,\n',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#\n',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#\n',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,\n',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,\n'
							];
	appendCode(mapByFlagOutput);

	appendHeading('removeRows(RowPurger.Predicate predicate):', 'h4');
	appendParagraph('It is useful when we want to remove rows from dataset for a given condition.');
	appendParagraph('Let say we want to remove those row whose field “direction” is Missed.Here is the code:');

	var removeRowCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");\n',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);\n',
						'TransformableRDD purged =  initialRDD.removeRows(new RowPurger.Predicate(){\n',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override\n',
							'&nbsp;&nbsp;&nbsp;&nbsppublic Boolean evaluate(RowRecord row) {\n',
							'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspreturn row.get(2).equals("Missed");\n',
						'&nbsp;&nbsp;&nbsp;&nbsp;}\n',
						'});\n',
						'purged.saveAsTextFile("output");\n'
						];
	appendCode(removeRowCode);
	appendParagraph('In above code will write file which will be look like:');
	var removeRowOutput = ['07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011\n',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011\n',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010'
							];
	appendCode(removeRowOutput);

	appendHeading('clusters(int columnIndex, ClusteringAlgorithm algorithm)');
	appendParagraph('In this method we have to pass the clustering algorithm by which we want to group the similar item in given column index.');
	appendParagraph('We are introducing three algorithm for clustering');
	var imputationList = ['Simple Fingerprint algorithm\n',
							'N-Gram Fingerprint algorithm\n',
							'Levenshtein distance algorithm\n'
							];
	appendList(imputationList);
	appendHeading('By Simple fingerprint algorithm:', 'h4');
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new SimpleFingerprintAlgorithm());']);
	appendHeading('By N-Gram Fingerprint algorithm :', 'h4');
	appendParagraph('In this algorithm we pass the value of n which is the size of chars of the token.')
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new NGramFingerprintAlgorithm(2));']);
	appendHeading('By Levenshtein distance algorithm:');
	appendParagraph('This algorithm groups the item of field if distance between them is very less.  ');
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new LevenshteinDistance());']);
							;
}



$(document).ready(appendAll)
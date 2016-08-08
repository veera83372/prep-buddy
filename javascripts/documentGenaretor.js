var appendHeading = function(heading, headTag, id) {
	id = id || '';
	var haddingHTML = '<headTag id="ID">heading</headTag>';
	var replacement = {
		'heading' : heading,
		'headTag' : headTag,
		'ID' : id
	}
	haddingHTML = haddingHTML.replace(/heading|headTag|ID/g, function(x) {
		return 	replacement[x];
	});
	$('.documentation').append(haddingHTML);
}

var appendList = function(list, className) {
	var html = '<ul class="className">';
	for (var i = 0; i < list.length; i++) {
		html = html + '<li>' + list[i] + '</li>';
	}
	html = html + '</ul>';
	html = html.replace(/className/, className);
	$('.documentation').append(html);
}

var codeDivWithButtons =  '<div class="codeMenu"> <button class="scala codeButton btnClicked"> Scala </button> <button class="java codeButton"> Java </button> </div>'
var appendCode = function() {
	var id = 0;
	return function(scalaCode, javaCode){
		scalaCodeClass = "scalaCode" + id
		javaCodeClass = "javaCode" + id
		var html = '<div class="codeMainBlock">' + codeDivWithButtons +'<div class="code"><div class="ScalaDoc ' + scalaCodeClass + '"></div><div class="JavaDoc zero hidden ' + javaCodeClass + '"></div> </div></div>';
		$('.documentation').append(html);
		var scalaHtml = ""
		var javaHtml = ""
		scalaCode.forEach(function(eachLine) {
			scalaHtml += '<p>' + eachLine + '</p>';	
		})
		javaCode.forEach(function(oneLine) {
			javaHtml += '<p>' + oneLine + '</p>'
		})
		$('.' + scalaCodeClass).append(scalaHtml)
		$('.' + javaCodeClass).append(javaHtml)
		id ++
	}
}()

var appendParagraph = function(toAppend) {
	var html = '<p>' + toAppend + '</p>';
	$('.documentation').append(html);	
}

var appendExample = function(list) {
	var html = '<div class="code"><p>';
	for (var i = 0; i < list.length; i++) {
		html += list[i] + '<br>';
	}
	html += '</div>';
	$('.documentation').append(html);
}


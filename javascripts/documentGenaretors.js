var appendHeading = function(heading, headTag) {
	var haddingHTML = '<headTag>heading</headTag>'
	var replacement = {
		'heading' : heading,
		'headTag' : headTag
	}
	haddingHTML = haddingHTML.replace(/heading|headTag/g, function(x) {
		return 	replacement[x];
	})
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
// var codeMenu =  '<ul class="menu"> <li class="item dropbtn"><div class="scala">Scala</div></li> <li class="item dropbtn"> <div class="Java">Java</div></li> </ul>'
var codeDivWithButtons =  '<div class="codeMenu"> <button class="scala codeButton"> Scala </button> <button class="java codeButton"> Java </button> </div>'
var appendCode = function() {
	var id = 0;
	return function(scalaCode, javaCode){
		scalaCodeClass = "scalaCode" + id
		javaCodeClass = "javaCode" + id
		var html = '<div class="code">' + codeDivWithButtons + '<div class="ScalaDoc ' + scalaCodeClass + '"></div><div class="JavaDoc zero hidden ' + javaCodeClass + '"></div> </div>';
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


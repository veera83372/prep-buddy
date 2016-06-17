var appendHeading = function(heading, headTag) {
	var haddingHTML = '<headTag>heading</headTag>'
	var replacement = {
		'heading' : heading,
		'headTag' : headTag
	}
	haddingHTML = haddingHTML.replace(/heading|headTag/g, function(x) {
		return 	replacement[x];
	})
	$('.main').append(haddingHTML);
}

var appendList = function(list, className) {
	var html = '<ul class="className">';
	for (var i = 0; i < list.length; i++) {
		html = html + '<li>' + list[i] + '</li>';
	}
	html = html + '</ul>';
	html = html.replace(/className/, className);
	$('.main').append(html);
}

var appendCode = function(list) {
	var html = '<div class="code"><p>';
	// var list = toAppend.split('\n');
	for (var i = 0; i < list.length; i++) {
		html += list[i] + '<br>';
	}
	html += '</div>';
	$('.main').append(html);
}

var appendParagraph = function(toAppend) {
	var html = '<p>' + toAppend + '</p>';
	$('.main').append(html);	
}


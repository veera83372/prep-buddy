package org.apache.prepbuddy.facets;

import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class FingerPrint {

        static final Pattern punctctrl = Pattern.compile("\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]");


        public static String key(String columnValue) {

            columnValue = columnValue.trim();
            columnValue = columnValue.toLowerCase();
            columnValue = punctctrl.matcher(columnValue).replaceAll("");
            String[] frags = StringUtils.split(columnValue);


            TreeSet<String> set = new TreeSet<String>();
            for (String ss : frags) {
                set.add(ss);
            }
            StringBuffer buffer = new StringBuffer();
            Iterator<String> iterator = set.iterator();

            while (iterator.hasNext()) {
                buffer.append(iterator.next());
                if (iterator.hasNext())
                    buffer.append(' ');
            }

            return buffer.toString();
        }
}

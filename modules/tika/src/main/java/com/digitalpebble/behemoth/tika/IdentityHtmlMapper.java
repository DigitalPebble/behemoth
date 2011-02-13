package com.digitalpebble.behemoth.tika;

import java.util.Locale;

import org.apache.tika.parser.html.HtmlMapper;

/**
 * Alternative HTML mapping rules that pass the input HTML as-is without any
 * modifications.
 * 
 * @since Apache Tika 0.8
 */
public class IdentityHtmlMapper implements HtmlMapper {

    public static final HtmlMapper INSTANCE = new IdentityHtmlMapper();

    public boolean isDiscardElement(String name) {
        return false;
    }

    public String mapSafeAttribute(String elementName, String attributeName) {
        return attributeName.toLowerCase(Locale.ENGLISH);
    }

    public String mapSafeElement(String name) {
        return name.toLowerCase(Locale.ENGLISH);
    }

}

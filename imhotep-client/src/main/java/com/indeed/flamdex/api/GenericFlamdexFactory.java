package com.indeed.flamdex.api;

import java.io.IOException;

/**
 * @author jsgroth
 */

public interface GenericFlamdexFactory {
    IntTermIterator createIntTermIterator(String termsFilePath, String docsFilePath) throws IOException;
    String getIntTermsFilename(String field);
    String getIntDocsFilename(String field);

    StringTermIterator createStringTermIterator(String termsFilePath, String docsFilePath) throws IOException;
    String getStringTermsFilename(String field);
    String getStringDocsFilename(String field);

    DocIdStream createDocIdStream();
}

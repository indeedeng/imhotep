package com.indeed.flamdex;

import com.indeed.flamdex.reader.MockFlamdexReader;

import java.io.IOException;

/**
 * @author jsgroth
 */
public class MakeAFlamdex {
    public static MockFlamdexReader make() {
        try {
            return YamlFlamdexParser.parseFromClasspathResource("test.yml");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

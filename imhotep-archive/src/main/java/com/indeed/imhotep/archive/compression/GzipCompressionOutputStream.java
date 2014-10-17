/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author jsgroth
 */
public class GzipCompressionOutputStream extends CompressionOutputStream {
    public GzipCompressionOutputStream(OutputStream out) throws IOException {
        super(new ResettableGZIPOutputStream(out));
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        out.write(bytes, off, len);
    }

    @Override
    public void finish() throws IOException {
        ((ResettableGZIPOutputStream)out).finish();
    }

    @Override
    public void resetState() throws IOException {
        ((ResettableGZIPOutputStream)out).resetState();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    private static class ResettableGZIPOutputStream extends GZIPOutputStream {
        private ResettableGZIPOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        private void resetState() {
            def.reset();
        }
    }
}

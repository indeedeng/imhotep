/*
 * Copyright (C) 2018 Indeed Inc.
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
package com.indeed.imhotep;

import com.indeed.imhotep.protobuf.DatasetInfoMessage;
import java.util.Collection;

/**
 * @author jsgroth
 */
public class DatasetInfo {
    private final String dataset;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;

    public DatasetInfo(final String dataset,
                       final Collection<String> intFields,
                       final Collection<String> stringFields) {
        this.dataset = dataset;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    public String getDataset() {
        return dataset;
    }

    public Collection<String> getIntFields() {
        return intFields;
    }

    public Collection<String> getStringFields() {
        return stringFields;
    }

    public DatasetInfoMessage toProto() {

        return DatasetInfoMessage.newBuilder()
                .setDataset(dataset)
                .addAllIntField(intFields)
                .addAllStringField(stringFields)
                .build();
    }

    public static DatasetInfo fromProto(final DatasetInfoMessage proto) {
        return new DatasetInfo(
                proto.getDataset(),
                proto.getIntFieldList(),
                proto.getStringFieldList());
    }
}

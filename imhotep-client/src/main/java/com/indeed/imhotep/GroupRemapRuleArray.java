/*
 * Copyright (C) 2016 Indeed Inc.
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

import java.util.Iterator;

public class GroupRemapRuleArray {

    private final GroupRemapRule[] rules;

    public GroupRemapRuleArray(final int numRules,
                               final Iterator<GroupRemapRule> ruleIt) {
        rules = new GroupRemapRule[numRules];
        for (int idx = 0; idx < rules.length; idx++) {
            if (!ruleIt.hasNext()) {
                final String message =
                    "numRules is " + numRules +
                    " but iterator only had " + idx + " rules";
                throw new IllegalArgumentException(message);
            }
            rules[idx] = ruleIt.next();
        }
    }

    public GroupRemapRule[] elements() { return rules; }
}

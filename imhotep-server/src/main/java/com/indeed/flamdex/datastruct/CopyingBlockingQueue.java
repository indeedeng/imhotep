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

package com.indeed.flamdex.datastruct;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by darren on 4/21/15.
 */
public interface CopyingBlockingQueue<E> extends BlockingQueue<E> {
    void take(E dest) throws InterruptedException;

    void poll(long timeout, TimeUnit unit, E dest) throws InterruptedException;

    void poll(E dest);

    void removeCopy(E dest);

    void element(E dest);

    void peek(E dest);


    interface ObjFactory<E> {
        E newObj();

        E getNil();

        boolean equalsNil(E dest);
    }

    interface ObjCopier<E> {
        void copy(E dest, E src);
    }

}

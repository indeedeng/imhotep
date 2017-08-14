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

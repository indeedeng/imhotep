package com.indeed.flamdex.datastruct;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by darren on 4/21/15.
 */
public interface CopyingBlockingQueue<E> extends BlockingQueue<E> {
    public void take(E dest) throws InterruptedException;

    public void poll(long timeout, TimeUnit unit, E dest) throws InterruptedException;

    public void poll(E dest);

    public void removeCopy(E dest);

    public void element(E dest);

    public void peek(E dest);


    public interface ObjFactory<E> {
        E newObj();

        E getNil();

        boolean equalsNil(E dest);
    }

    public interface ObjCopier<E> {
        void copy(E dest, E src);
    }

}

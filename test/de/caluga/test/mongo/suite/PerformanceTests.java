package de.caluga.test.mongo.suite;/**
 * Created by stephan on 23.11.15.
 */

import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * TODO: Add Documentation here
 **/
public class PerformanceTests {
    int threadCount = 0;

    @Test
    public void testit() throws Exception {
        List<String> lst = new ArrayList<>();
        System.out.println("\nTesting with ArraList");
        testIt(lst);

        lst = new Vector<>();
        System.out.println("\nTesting with Vector");
        testIt(lst);

        lst = new LinkedList<>();
        System.out.println("\nTesting with LinkedList");
        testIt(lst);


    }

    private void testIt(final List<String> lst) {
        long start = System.currentTimeMillis();
        int threads = 300;
        threadCount = 0;
        for (int i = 0; i < threads; i++) {
            final int j = i;
            new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (lst) {
                        try {
                            lst.add("hello " + j + " - " + k);
                        } catch (Exception e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            }.start();
        }

        while (threadCount < threads) {
//            System.out.println("Waiting..");
            Thread.yield();
        }
        long dur = System.currentTimeMillis() - start;
        System.out.println("write took : " + dur);
        System.out.println("Counting   : " + lst.size() + " missing: " + ((threads * 1000) - lst.size()));
        threadCount = 0;
        start = System.currentTimeMillis();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (lst) {
                        try {
                            if (j * 1000 + k < lst.size())
                                lst.get(j * 1000 + k);
                        } catch (Exception e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            }.start();
        }

        while (threadCount < threads) {
//            System.out.println("Waiting for reads..");
            Thread.yield();
        }
        dur = System.currentTimeMillis() - start;
        System.out.println("read took : " + dur);
    }

}

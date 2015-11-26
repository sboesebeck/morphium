package de.caluga.test.mongo.suite;/**
 * Created by stephan on 23.11.15.
 */

import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: Add Documentation here
 **/
public class PerformanceTests {
    int threadCount = 0;


    @Test
    public void hashmapVsHashtableTest() throws Exception {
        System.out.println("Testing with HashMap: ");
        testIt(new HashMap<String, Object>());

        System.out.println("\nTesting with Hashtable: ");
        testIt(new Hashtable<String, Object>());

        System.out.println("\nTesting with LinkedHashmap: ");
        testIt(new LinkedHashMap<String, Object>());

        System.out.println("\nTesting with ConcurrentHashMap: ");
        testIt(new ConcurrentHashMap<String, Object>());


    }

    private void testIt(final Map<String, Object> map) {
        long start = System.currentTimeMillis();
        int threads = 300;
        threadCount = 0;
        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (map) {
                        try {
                            map.put("hello " + j + " - " + k, j + k);
                        } catch (Throwable e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            };
            thr.add(t);
            t.start();
        }
        System.out.println("Waiting for threads...");
        for (Thread t : thr)
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        long dur = System.currentTimeMillis() - start;
        System.out.println("write took          : " + dur);
        int max = (threads * 1000);
        int missing = max - map.size();
        System.out.println("Counting            : " + map.size() + " missing: " + missing);

        threadCount = 0;
        thr = new ArrayList<>();
        start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (map) {
                        try {
                            map.get("hello " + j + " - " + k);
                        } catch (Throwable e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            };
            thr.add(t);
            t.start();
        }
//        System.out.println("Waiting for threads...");

        for (Thread t : thr)
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//        while (threadCount < threads) {
//            System.out.println("Waiting for reads..");
//            Thread.yield();
//        }
        dur = System.currentTimeMillis() - start;
        double all = ((double) missing) / ((double) max / (double) dur);
        System.out.println("read took           : " + dur);
        System.out.println("addition for missing: " + all + "ms");
    }


    @Test
    public void testListVsVector() throws Exception {
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

    @Test
    public void testCreation() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            new Vector<String>();
        }
        long dur = System.currentTimeMillis() - start;
        System.out.println("Duration vector   : " + dur + "ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            new ArrayList<String>();
        }
        dur = System.currentTimeMillis() - start;
        System.out.println("Duration ArrayList: " + dur + "ms");
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            new LinkedList<String>();
        }
        dur = System.currentTimeMillis() - start;
        System.out.println("Duration LinkedList: " + dur + "ms");
    }

    private void testIt(final List<String> lst) {
        long start = System.currentTimeMillis();
        int threads = 300;
        threadCount = 0;
        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (lst) {
                        try {
                            lst.add("hello " + j + " - " + k);
                        } catch (Throwable e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            };
            thr.add(t);
            t.start();
        }

        for (Thread t : thr)
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        long dur = System.currentTimeMillis() - start;
        System.out.println("write took : " + dur);
        System.out.println("Counting   : " + lst.size() + " missing: " + ((threads * 1000) - lst.size()));
        thr = new ArrayList<>();
        threadCount = 0;
        start = System.currentTimeMillis();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                public void run() {
                    for (int k = 0; k < 1000; k++) {
//                        synchronized (lst) {
                        try {
                            if (j * 1000 + k < lst.size())
                                lst.get(j * 1000 + k);
                        } catch (Throwable e) {
                            //ignore
                        }
//                        }
                    }
                    threadCount++;
                }
            };
            thr.add(t);
            t.start();
        }

        for (Thread t : thr)
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        dur = System.currentTimeMillis() - start;
        System.out.println("read took : " + dur);
    }

}

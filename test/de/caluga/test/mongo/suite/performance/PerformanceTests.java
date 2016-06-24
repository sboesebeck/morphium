package de.caluga.test.mongo.suite.performance;/**
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
        int retries = 20;

        System.out.println("Testing with HashMap: ");
        repeatTest(retries, new HashMap<>());
        System.out.println("\nTesting with LinkedHashmap: ");
        repeatTest(retries, new LinkedHashMap<>());


        System.out.println("\nTesting with Hashtable: ");
        repeatTest(retries, new Hashtable<>());
        System.out.println("\nTesting with ConcurrentHashMap: ");
        repeatTest(retries, new ConcurrentHashMap<>());


    }

    private void repeatTest(int retries, Map<String, Object> m) {
        long durWr = 0;
        long durRd = 0;
        for (int i = 0; i < retries; i++) {
            m.clear();
            durWr += testWrite(m);
            durRd += testRead(m);
        }
        System.out.println("Average write duration: " + (durWr / retries) + " ms");
        System.out.println("Average read duration: " + (durRd / retries) + " ms");
    }

    private long testRead(final Map<String, Object> map) {
        long threads = 300;
        long dur = 0;
        threadCount = 0;
        List<Thread> thr = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                @Override
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
        //        System.out.println("read took           : " + dur);
        return dur;
    }


    private long testWrite(final Map<String, Object> map) {
        long start = System.currentTimeMillis();
        int threads = 300;
        threadCount = 0;
        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                @Override
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
        //        System.out.println("Waiting for threads...");
        for (Thread t : thr)
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        long dur = System.currentTimeMillis() - start;
        //        System.out.println("write took          : " + dur);
        int max = (threads * 1000);
        int missing = max - map.size();
        //        System.out.println("Counting            : " + map.size() + " missing: " + missing);

        return dur;
    }


    @Test
    public void testListVsVector() throws Exception {
        List<String> lst = new ArrayList<>();
        System.out.println("\nTesting with ArraList");
        testWrite(lst);

        lst = new Vector<>();
        System.out.println("\nTesting with Vector");
        testWrite(lst);

        lst = new LinkedList<>();
        System.out.println("\nTesting with LinkedList");
        testWrite(lst);


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

    private void testWrite(final List<String> lst) {
        long start = System.currentTimeMillis();
        int threads = 300;
        threadCount = 0;
        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int j = i;
            Thread t = new Thread() {
                @Override
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
                @Override
                public void run() {
                    for (int k = 0; k < 1000; k++) {
                        //                        synchronized (lst) {
                        try {
                            if (j * 1000 + k < lst.size()) {
                                lst.get(j * 1000 + k);
                            }
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

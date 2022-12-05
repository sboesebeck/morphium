package de.caluga.test.mongo.suite.locking;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class MonitorTest extends MorphiumTestBase {

    public void complexQueueTest() throws Exception {
        //Preparing
        Monitor m = new Monitor();
        m.setId("msg1");
        m.setCount(4); //maxparallel
        morphium.store(m);
        m = new Monitor();
        m.setId("msg4");
        m.setCount(1);
        morphium.store(m);
        AtomicBoolean running = new AtomicBoolean(true);

        //now the threads

        for (int i = 0; i < 25; i++) {
            String thrId = "Thr" + i;
            new Thread(()->{
                while (running.get()) {
                    //nothing todo -> poll
                    var q = morphium.createQueryFor(ToDo.class).limit(1);
                    List<ToDo> todos = null;

                    try {
                        todos = morphium.lockEntities(q, thrId, 1000);
                    } catch (MorphiumDriverException e1) {
                        log.error("Error locking", e1);
                    }

                    if (todos == null || todos.isEmpty()) {
                        log.info("No todo found...");

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }

                        continue;
                    }

                    for (ToDo td : todos) {
                        //lock and rolll
                        try {
                            var t = morphium.lockEntity(td, thrId, 5000);

                            if (t == null) {
                                log.info("Could not lock todo...skipping");
                                continue;
                            }

                            if (morphium.createQueryFor(Monitor.class).f("_id").eq(t.getJob()).countAll() > 0) {
                                var mon = morphium.createQueryFor(Monitor.class).f("_id").eq(t.getJob()).get();

                                if (mon != null) {
                                    while (mon.getCount() >= 1) {
                                        var res = q.q().f("_id").eq(mon.getId()).f("count").eq(mon.getCount()).dec("count",1);

                                        if (!res.get("nModified").equals(Integer.valueOf(1))) {
                                            //got log
                                            mon=morphium.reread(mon); //get the current value
                                            if (mon.getCount()==0){
                                                log.error("This should not happen!");
                                                break;
                                            }
                                        }
                                    }
                                }

                                //no monitor for job
                            }
                         } catch (MorphiumDriverException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
                log.info(thrId + "Thread ended");
            }).start();
        }
    }

    @Entity
    @Lockable
    public static class ToDo {
        @Id
        private MorphiumId id;
        private String job;

        @LockedAt
        private long lockedAt;
        @LockedBy
        private String lockedBy;

        public MorphiumId getId() {
            return id;
        }
        public void setId(MorphiumId id) {
            this.id = id;
        }
        public String getJob() {
            return job;
        }
        public void setJob(String job) {
            this.job = job;
        }
        public long getLockedAt() {
            return lockedAt;
        }
        public void setLockedAt(long lockedAt) {
            this.lockedAt = lockedAt;
        }
        public String getLockedBy() {
            return lockedBy;
        }
        public void setLockedBy(String lockedBy) {
            this.lockedBy = lockedBy;
        }

    }
    @Entity
    @Lockable
    public static class Monitor {
        @Id
        private String id;
        @LockedBy
        private String lockedBy;
        @LockedAt
        private long lockedAt;

        private int count;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getLockedBy() {
            return lockedBy;
        }

        public void setLockedBy(String lockedBy) {
            this.lockedBy = lockedBy;
        }

        public long getLockedAt() {
            return lockedAt;
        }

        public void setLockedAt(long lockedAt) {
            this.lockedAt = lockedAt;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

    }
}

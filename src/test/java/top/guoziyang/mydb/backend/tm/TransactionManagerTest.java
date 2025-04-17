package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

/**
 * TransactionManager 的测试类，主要用于多线程环境下的事务管理测试。
 */
public class TransactionManagerTest {

    // 安全随机数生成器，用于生成测试数据。
    static Random random = new SecureRandom();

    // 记录当前事务的数量。
    private int transCnt = 0;
    // 工作线程的数量。
    private int noWorkers = 50;
    // 每个工作线程需要执行的任务数量。
    private int noWorks = 3000;
    // 用于同步的锁。
    private Lock lock = new ReentrantLock();
    // 事务管理器实例。
    private TransactionManager tmger;
    // 用于存储事务ID和事务状态的映射。
    private Map<Long, Byte> transMap;
    // 用于等待所有工作线程完成的计数器。
    private CountDownLatch cdl;

    /**
     * 测试多线程环境下事务管理器的性能和正确性。
     */
    @Test
    public void testMultiThread() {
        // 初始化事务管理器。
        tmger = TransactionManager.create("D:/Code/MYDB/tmp/mydb/tranmger_test");
        // 初始化事务状态映射表。
        transMap = new ConcurrentHashMap<>();
        // 初始化计数器，用于等待所有工作线程完成。
        cdl = new CountDownLatch(noWorkers);
        // 创建并启动多个工作线程。
        for(int i = 0; i < noWorkers; i ++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        // 等待所有工作线程完成。
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 测试完成后，删除事务管理文件。
        assert new File("D:/Code/MYDB/tmp/mydb/tranmger_test.xid").delete();
    }

    /**
     * 工作线程执行的事务操作方法。
     */
    private void worker() {
        // 记录当前线程是否在事务中。
        boolean inTrans = false;
        // 当前事务的ID。
        long transXID = 0;
        // 模拟事务操作。
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if(op == 0) {
                // 对事务的开始和结束进行同步控制。
                lock.lock();
                if(inTrans == false) {
                    // 如果当前没有事务，则开始一个新的事务。
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    // 如果当前有事务，则根据随机数决定是提交还是回滚事务。
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                // 对事务状态的查询进行同步控制。
                lock.lock();
                if(transCnt > 0) {
                    // 随机选择一个事务，检查其状态。
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        // 工作线程完成任务，计数器减一。
        cdl.countDown();
    }
}

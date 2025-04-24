package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

public class TransactionManagerImpl implements TransactionManager {

    static final int LEN_XID_HEADER_LENGTH = 8; // XID文件头长度
    private static final int XID_FIELD_SIZE = 1; // 每个事务的占用长度
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;     // 事务进行中
    private static final byte FIELD_TRAN_COMMITTED = 1;  // 事务已提交
    private static final byte FIELD_TRAN_ABORTED = 2;    // 事务已取消
    public static final long SUPER_XID = 0;    // 超级事务，永远为commited状态
    static final String XID_SUFFIX = ".xid";    // XID 文件后缀

    private RandomAccessFile file;  //允许以随机访问的方式读写文件
    private FileChannel fc;
    private long xidCounter; //xidCounter用于记录事务的数量
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock(); // 初始化锁, 用于线程安全的操作事务ID计数器
        checkXIDCounter();   // 校验并初始化 xidCounter
    }

    /**
     * 检查XID文件是否合法
     * 通过文件头的 8 字节数字反推文件的理论长度
     * 与文件的实际长度做对比。如果不同则认为 XID 文件不合法。
     */
    private void checkXIDCounter() {
        // 初始化文件长度变量
        long fileLen = 0;
        // 尝试获取文件长度
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            // 如果文件长度不可获取，抛出异常
            Panic.panic(Error.BadXIDFileException);
        }
        // 如果文件长度小于XID文件头长度，抛出异常
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        // 创建缓冲区以读取XID文件头
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        // 尝试读取文件头
        try {
            fc.position(0);   // 将文件通道的位置设置为0
            fc.read(buf);   // 从文件通道读取数据到ByteBuffer
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 将ByteBuffer的内容解析为长整型，作为xidCounter
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算xidCounter+1对应的XID位置
        long end = getXidPosition(this.xidCounter + 1);
        // 如果计算出的XID位置与文件长度不符，抛出异常
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
        //假设xid=9，return 8+（9-1）*1=16
        //表示事务 ID 为 9 的事务状态在文件中的位置是第 16 字节。
    }


    //开启一个事务
    public long begin() {
        counterLock.lock();
        try {
            // 生成一个新的事务id
            long xid = xidCounter + 1;
            // 调用updateXID方法，将新的事务ID和事务状态（这里是活动状态）写入到XID文件中
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();// 调用incrXIDCounter方法，将事务计数器加1，并更新XID文件的头部信息
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);  // 获取xid事务在文件中的位置
        byte[] tmp = new byte[XID_FIELD_SIZE];  // 创建一个用于存储xid事务状态的字节数组
        tmp[0] = status;  // 将事务状态值写入字节数组的第一个位置
        ByteBuffer buf = ByteBuffer.wrap(tmp);  // 使用ByteBuffer包装字节数组以进行I/O操作
        try {
            fc.position(offset);  // 设置文件通道的位置到事务状态需要更新的位置
            fc.write(buf);        // 将事务状态写入文件通道
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false); // 确保文件通道的更改持久化到存储设备
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    // 全局更新计数器-将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter++;
        // 将新的XID计数器转换为字节格式，以便写入文件
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        // 将新的 xidCounter 写回文件头
        try {
            // 设置文件通道位置到文件头
            fc.position(0);
            // 写入更新的XID头信息
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制将文件通道的内容写入存储设备
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}

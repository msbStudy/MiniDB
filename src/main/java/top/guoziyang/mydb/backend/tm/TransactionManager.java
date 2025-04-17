package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

//提供接口供其他模块调用，用来创建事务和查询事务状态
public interface TransactionManager {
    long begin();                       // 开启一个新事务
    void commit(long xid);              // 提交一个事务
    void abort(long xid);               // 撤销一个事务
    boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交
    boolean isAborted(long xid);        // 查询一个事务的状态是否是已取消
    void close();                       // 关闭TM


    //创建xid文件
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    //打开xid文件
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}

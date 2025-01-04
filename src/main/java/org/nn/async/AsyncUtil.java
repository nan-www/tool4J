package org.nn.async;

import com.google.common.base.Stopwatch;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class AsyncUtil {
    private final Executor commonPoolExecutor;

    private static final Logger log = LoggerFactory.getLogger(AsyncUtil.class);

    public AsyncUtil(Executor commonPoolExecutor) {
        this.commonPoolExecutor = commonPoolExecutor;
    }

    /**
     * 在给定的时间内获取第一个执行成功并且符合 judgeFunction的结果。
     * @apiNote 如果全部任务输出的结果都不符合调用方的需求则返回第一个完成的任务的结果。
     * @param function 该方法内请不要捕获异常。如果在function中自行捕获了异常那么该方法等同于 {@link CompletableFuture#anyOf(CompletableFuture[])}
     * @param timeout  单位毫秒
     * @return Null 代表在规定时间内全都超时了，需要调用方特别判断。
     */
    public <T, R> R batchSupplyAsyncAndGetSuccessOne(List<T> params, Function<T, R> function, long timeout, Function<R, Boolean> judgeFunction) {
        if (CollectionUtils.isEmpty(params)) {
            return null;
        }
        // Should be fair lock here.
        ReentrantLock dealResultLock = new ReentrantLock(true);
        CountDownLatch finishLatch = new CountDownLatch(1);
        AtomicInteger reqCount = new AtomicInteger(params.size());
        AtomicReference<R> returnValue = new AtomicReference<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (T param : params) {
            CompletableFuture
                    .supplyAsync(() -> function.apply(param), commonPoolExecutor)
                    .whenComplete((r, ex) -> {
                        try {
                            dealResultLock.lock();
                            int afterThisReq = reqCount.decrementAndGet();
                            if (ex != null) {
                                // Notify main thread if all tasks completed exceptionally.
                                if (afterThisReq == 0) {
                                    finishLatch.countDown();
                                }
                                return;
                            }
                            // Double check. Quickly release other threads.
                            if (finishLatch.getCount() == 0) {
                                return;
                            }
                            if (returnValue.get() == null) {
                                // Record default value and return when all tasks get failed result.
                                returnValue.set(r);
                            }
                            if (judgeFunction.apply(r)) {
                                // Record the first successful result.
                                returnValue.set(r);
                                log.info("batchSupplyAsyncAndGetSuccessOne success. param:{}, result:{} cost:{}", param, r, stopwatch.elapsed());
                                finishLatch.countDown();
                            }
                            if (afterThisReq == 0) {
                                // The last task is responsible for waking up the main thread.
                                finishLatch.countDown();
                            }
                        }finally {
                            dealResultLock.unlock();
                        }
                    })
                    .exceptionally((ex) -> {
                        // abandon exception.
                        log.error("batchSupplyAsyncAndGetSuccessOne failed, param:{}", param, ex);
                        // default return null.
                        return null;
                    });
        }
        try {
            if (finishLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                return returnValue.get();
            }else{
                log.error("batchSupplyAsyncAndGetOne time out. params:{}", params);
                return null;
            }
        } catch (InterruptedException e) {
            log.error("batchSupplyAsyncAndGetOne interrupted, params:{}", params, e);
            throw new RuntimeException();
        }
    }
}

package com.yi.common.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGTask implements Callable<DAGTask.Result> {

    String name ;
    private volatile int status =0; //0开始 1运行中 2结束
    private Object lock = new Object();
    private long timeout = 10;
    private volatile Result result;
    private Callable callable;
    public List<CountDownLatch>  childSemphore = new ArrayList<>();
    CountDownLatch semaphore;

    public DAGTask(String name ,Callable callable,long timeout) {
        this.name = name;
        this.callable = callable;
        this.timeout = timeout;
    }


    public DAGTask(String name ,Callable callable) {
        this.name = name;
        this.callable = callable;
    }


    public DAGTask(String name ) {
        this.name = name;
        this.callable = callable;
    }
    public void setSemaphore(CountDownLatch semaphore){
        this.semaphore = semaphore;
    }


    public void addChildSemphore(CountDownLatch semaphore){
        this.childSemphore.add( semaphore) ;
    }


    @Override
    public Result call() throws Exception {
        if(status ==2){
            System.out.println(name +" already run over");
            return result ;
        }
        if(status==1){
            System.out.println(name +" running,wait");
            synchronized (lock){
                lock.wait();
            }
            return call();
        }
        status =1;
        System.out.println(name +" start run");
        if(semaphore !=null){
            semaphore.await();
        }
        result = runTask();
        for(CountDownLatch child :childSemphore){
            child.countDown();
        }
        System.out.println(name +" notify child over");
        synchronized (lock) {
            lock.notifyAll();
        }
        System.out.println(name +" run complete");
        status = 2;
        return result;
    }

    private Result runTask(){
        System.out.println(name +" run--------------");
        if(callable !=null){
            FutureTask futureTask = new FutureTask(callable);
            try {
                futureTask.get(timeout, TimeUnit.SECONDS);
                Object object=  callable.call();
                return new Result(object);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Result(name);
    }

    @Override
    public String toString() {
        return "DAGTask{" +
                "name='" + name + '\'' +
                '}';
    }



    public static class Result {

        Object result;


        public Result(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return result;
        }

        public void setResult(Object result) {
            this.result = result;
        }
    }

}

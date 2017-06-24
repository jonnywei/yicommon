package com.yi.common.scheduler;

import com.yi.common.ds.DAG;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGTaskNode<T,V> extends DAG.Node<T,V> implements Callable<V> {

    String name ;
    private volatile int notifyChild =0; //0未通知 1通知
    private long timeout = 30;
    public volatile  FutureTask<V> futureTask;
    public List<CountDownLatch>  childSemphore = new ArrayList<>();
    CountDownLatch semaphore;
    public  ExecutorService executor;
    private volatile boolean error = false;

    public  Callable<V> PARENT_ERROR_RUN = new Callable<V>() {
        @Override
        public V call() throws Exception {
            return null;
        }
    };

    public DAGTaskNode(String name , Callable<V> callable, long timeout) {
        this.name = name;
        this.futureTask = new FutureTask<V>(callable);
        this.timeout = timeout;
    }


    public DAGTaskNode(String name , Callable callable) {
        this.name = name;
        this.futureTask = new FutureTask<V>(callable);
    }


    public void setSemaphore(CountDownLatch semaphore){
        this.semaphore = semaphore;
    }


    public void addChildSemphore(CountDownLatch semaphore){
        this.childSemphore.add( semaphore) ;
    }


    @Override
    public V call() throws Exception {
        if(futureTask.isDone()){
            System.out.println(name +" already run over");
            return futureTask.get();
        }
        System.out.println(name +" start run");
        if(semaphore !=null){
            semaphore.await();
        }
        V result = null;
        if(parentHaveError()){
            error = true;
            futureTask = new FutureTask<V>(PARENT_ERROR_RUN);
            System.out.println(name +" parent have error, skip execute");

        }
        result = executeTask();

        if(notifyChild == 0){
            for(CountDownLatch child : childSemphore){
                child.countDown();
            }
            System.out.println(name +" notify child over");
            notifyChild = 1;
        }
        System.out.println(name +" run complete");
        return result;
    }

    public boolean parentHaveError(){
        for(DAGTaskNode<T,V> p : parents){
            if(p.error){
                return true;
            }
        }
        return false;
    }


    private V executeTask()  {
        executor.submit(futureTask); //
        try {
            return futureTask.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            error = true;
        } catch (ExecutionException e) {
            e.printStackTrace();
            error = true;

        } catch (TimeoutException e) {
            e.printStackTrace();
            error = true;
        }
        return null;
    }


    @Override
    public String toString() {
        return "DAGTaskNode{" +
                "name='" + name + '\'' +
                '}';
    }



}

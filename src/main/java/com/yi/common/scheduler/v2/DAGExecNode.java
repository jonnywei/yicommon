package com.yi.common.scheduler.v2;

import com.yi.common.ds.v2.DAG;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGExecNode<T,V> extends DAG.Node<T,V> implements Callable<V> {

    String name ;
    private volatile int status  =0; // 0 初始化，1 执行中，2，执行完毕
    private long timeout = 30;
    public  ExecutorService executor;
    private volatile boolean error = false;
    private volatile  V result;
    private Callable<V> callable;
    private CountDownLatch finish = new CountDownLatch(1);

    public DAGExecNode(String name , Callable<V> callable, long timeout) {
        this.name = name;
        this.timeout = timeout;
        this.callable = callable;
    }


    public DAGExecNode(String name , Callable callable) {
        this.name = name;
        this.callable = callable;

    }

    @Override
    public V call() throws Exception {
        if(status == 2){
            System.out.println(name +" already run over");
            return result;
        }
        status =1;
        System.out.println(name +" start run");

        if(parentHaveError()){
            error = true;
            System.out.println(name +" parent have error, skip execute");

        }
        result = executeTask();
        status =2;

        for(DAGExecNode<T,V> childNode: this.successors){
            if(parentExecuteOver(childNode)){
                System.out.println(name +" trigger "+childNode.name +"execute");
                executor.submit(childNode);
            }
        }
        finish.countDown();
        System.out.println(name +" run complete");
        return result;
    }


    public boolean parentExecuteOver(DAGExecNode<T,V> childNode){
        for(DAGExecNode<T,V> p : childNode.parents){
            if(p.status !=2){
                return false;
            }
        }
        return true;
    }


    public boolean parentHaveError(){
        for(DAGExecNode<T,V> p : parents){
            if(p.error){
                return true;
            }
        }
        return false;
    }


    private V executeTask() throws Exception {
        return  callable.call();
    }


    @Override
    public String toString() {
        return "DAGTaskNode{" +
                "name='" + name + '\'' +
                '}';
    }


    public V getResult(long timeout, TimeUnit timeUnit) throws InterruptedException {
        finish.await(timeout, timeUnit);
        return result;
    }


}

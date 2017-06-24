package com.yi.common.scheduler;

import com.yi.common.ds.DAG;

import java.util.concurrent.*;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGScheduler<T extends Callable<V>, V> {

    ExecutorService executor = Executors.newCachedThreadPool();

    public int timeout = 20;

    DAG<T,V > dag ;

    public DAGScheduler(DAG<T,V> dag) {
        this.dag = dag;
        //增加当前信号量
        dag.visitDAG(new DAG.VisitFn<T,V>() {
            @Override
            public Object apply(DAGTaskNode<T,V> node) {
                 int psize = node.parents.size();
                if(psize ==0){
                    node.semaphore =null;
                }else {
                    node.semaphore = new CountDownLatch(psize);
                }
                node.executor = executor; //设置执行器
                return null;
            }
        } );
        //增加孩子信号量
        dag.visitDAG(new DAG.VisitFn<T,V>() {
            @Override
            public Object apply(DAGTaskNode<T,V> node) {
                for(Object child : node.successors) {
                    node.addChildSemphore(((DAGTaskNode<T,V>)child).semaphore);
                }

                return null;
            }
        } );
    }

    public Future<V>  schedule( T current)  {
        //
//        List<Future>
       //添加任务执行
        DAGTaskNode<T,V> dagTaskNode = dag.getNode(current);
        dag.dfsDAG(new DAG.VisitFn<T,V>() {
            @Override
            public Object apply(DAGTaskNode<T,V>  node) {
                Future future = executor.submit(node);
                return null;
            }
        },dagTaskNode);
       return dagTaskNode.futureTask;

    }

    public Future<V>  scheduleWait( T current)  {
        //
//        List<Future>
        //添加任务执行
        DAGTaskNode<T,V> dagTaskNode = dag.getNode(current);
        dag.dfsDAG(new DAG.VisitFn<T,V>() {
            @Override
            public Object apply(DAGTaskNode<T,V>  node) {
                Future future = executor.submit(node);
                return null;
            }
        },dagTaskNode);
        return dagTaskNode.futureTask;

    }


    public void awitTermination(){
        try {
            executor.awaitTermination(dag.vertexCount() *timeout,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

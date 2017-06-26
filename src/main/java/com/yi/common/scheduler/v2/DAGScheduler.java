package com.yi.common.scheduler.v2;



import com.yi.common.ds.v2.DAG;

import java.util.ArrayList;
import java.util.List;
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
            public Object apply(DAGExecNode<T,V> node) {
                node.executor = executor;     //设置执行器
                return null;
            }
        } );
//        //增加孩子信号量
//        dag.visitDAG(new DAG.VisitFn<T,V>() {
//            @Override
//            public Object apply(DAGExecNode<T,V> node) {
//                for(Object child : node.successors) {
//                    node.addChildSemphore(((DAGExecNode<T,V>)child).semaphore);
//                }
//
//                return null;
//            }
//        } );
    }

    public V  schedule( T current) throws InterruptedException {
        //
//        List<Future>
       //添加任务执行
        DAGExecNode<T,V> dagTaskNode = dag.getNode(current);
        List<T> topo = dag.getTopo(current);
        List<DAGExecNode<T,V>> startNodes = new ArrayList<>();
        for(T x:topo){
            DAGExecNode<T,V> execNode = dag.getNode(x);
            if(execNode.parents.isEmpty()){
                startNodes.add(execNode);
            }
        }
        for(DAGExecNode execNode: startNodes){
            submitTask(execNode);
        }
        return dagTaskNode.getResult(100000,TimeUnit.SECONDS);

    }


    public void submitTask(DAGExecNode dagExecNode){
        Future<V> future = executor.submit(dagExecNode); //开始执行任务
    }

    public void awitTermination(){
        try {
            executor.awaitTermination(dag.vertexCount() *timeout,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

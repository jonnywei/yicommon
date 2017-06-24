package com.yi.common.scheduler;

import com.yi.common.ds.DAG;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGScheduler {

    ExecutorService executor = Executors.newCachedThreadPool();

    public int timeout = 20;

    DAG<DAGTask> dag ;

    public DAGScheduler(DAG<DAGTask> dag) {
        this.dag = dag;
        //增加当前信号量
        dag.visitDAG(new DAG.VisitFn<DAGTask>() {
            @Override
            public Object apply(DAG.Node<DAGTask> node) {
                int psize = node.parents.size();
                if(psize ==0){
                    node.vertex.semaphore =null;
                }else {
                    node.vertex.semaphore = new CountDownLatch(psize);
                }

                return null;
            }
        } );
        //增加孩子信号量
        dag.visitDAG(new DAG.VisitFn<DAGTask>() {
            @Override
            public Object apply(DAG.Node<DAGTask> node) {
                for(DAG.Node<DAGTask> child : node.successors) {
                    node.vertex.addChildSemphore(child.vertex.semaphore);
                }

                return null;
            }
        } );
    }

    public void schedule( DAGTask current){
        //
//        List<Future>
       //添加任务执行
        dag.dfsDAG(new DAG.VisitFn<DAGTask>() {
            @Override
            public Object apply(DAG.Node<DAGTask> node) {
                Future future = executor.submit(node.vertex);
                return null;
            }
        },dag.getNode(current));

    }

    public void awitTermination(){
        try {
            executor.awaitTermination(dag.vertexCount() *timeout,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

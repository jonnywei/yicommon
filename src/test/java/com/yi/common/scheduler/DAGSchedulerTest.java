package com.yi.common.scheduler;

import com.yi.common.ds.DAG;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGSchedulerTest {

    Callable callable = new Callable() {
        @Override
        public Object call() throws Exception {
            Random random = new Random();
            Thread.sleep(random.nextInt(20) * 1000);
            return  null;
        }
    };

    DAG<DAGTask> dag = new DAG<>();
    DAGTask i= new DAGTask("i",callable);
    DAGTask k= new DAGTask("k",callable);

    DAGTask x= new DAGTask("x",callable);
    DAGTask y= new DAGTask("y",callable);


    DAGTask u= new DAGTask("u",callable);
    DAGTask v= new DAGTask("v",callable);




    @Test
    public  void  runTest(){

        DAGScheduler dagScheduler = new DAGScheduler(dag);


        try {
            dagScheduler.schedule(i);
//            Thread.sleep(10000000);
            dagScheduler.schedule(k);
            dagScheduler.schedule(v);
            dagScheduler.schedule(x);
            dagScheduler.schedule(y);

            dagScheduler.awitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Before
    public  void  buildTaskDAG(){
        DAGTask a= new DAGTask("a",callable);
        DAGTask b= new DAGTask("b",callable);
        DAGTask c= new DAGTask("c",callable);
        DAGTask d= new DAGTask("d",callable);
        DAGTask e= new DAGTask("e",callable);
        DAGTask f= new DAGTask("f",callable);
        DAGTask g= new DAGTask("g",callable);
        DAGTask h= new DAGTask("h",callable);
        DAGTask j= new DAGTask("j",callable);

        dag.addEdge(new DAG.Edge(h,i));
        dag.addEdge(new DAG.Edge(a,b));
        dag.addEdge(new DAG.Edge(b,c));
        dag.addEdge(new DAG.Edge(c,e));
        dag.addEdge(new DAG.Edge(d,h));
        dag.addEdge(new DAG.Edge(g,f));
        dag.addEdge(new DAG.Edge(f,h));
        dag.addEdge(new DAG.Edge(e,i));

        dag.addEdge(new DAG.Edge(h,j));
        dag.addEdge(new DAG.Edge(j,k));


        //单独节点
        dag.addVertex(x);
        dag.addVertex(y);

        //单线
        dag.addEdge(new DAG.Edge(u,v));

        dag.printTopo(i);
        System.out.println("===================");
        //---
        dag.printTopo(k);

    }

}

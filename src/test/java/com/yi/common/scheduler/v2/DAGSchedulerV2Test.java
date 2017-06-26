package com.yi.common.scheduler.v2;

import com.yi.common.ds.v2.DAG;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by wjj on 6/23/17.
 */
public class DAGSchedulerV2Test {

    public class SleepTask implements Callable<String>{
        
        private String name;

        public SleepTask(String name) {
            this.name = name;
        }

        @Override
        public String call() throws Exception {
            Random random = new Random();
            System.out.println(name +" sleep ");
            Thread.sleep(random.nextInt(20) * 1000);
            return  name;
        }

        @Override
        public String toString() {
            return "SleepTask{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }
    

    DAG<SleepTask,String> dag = new DAG<>();
    SleepTask             i   = new SleepTask("i");
    SleepTask             k   = new SleepTask("k");

    SleepTask x= new SleepTask("x");
    SleepTask y= new SleepTask("y");


    SleepTask u= new SleepTask("u");
    SleepTask v= new SleepTask("v");




    @Test
    public  void  runTest(){

        DAGScheduler dagScheduler = new DAGScheduler(dag);


        try {
            Object value = dagScheduler.schedule(i);
//          Future<String> result =
//          String value =result.get();
          System.out.println("result="+value);
//            Thread.sleep(10000000);
//            dagScheduler.schedule(k);
//            dagScheduler.schedule(v);
//            dagScheduler.schedule(x);
//            dagScheduler.schedule(y);

//            dagScheduler.awitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Before
    public  void  buildTaskDAG(){
        SleepTask a= new SleepTask("a");
        SleepTask b= new SleepTask("b");
        SleepTask c= new SleepTask("c");
        SleepTask d= new SleepTask("d");
        SleepTask e= new SleepTask("e");
        SleepTask f= new SleepTask("f");
        SleepTask g= new SleepTask("g");
        SleepTask h= new SleepTask("h");
        SleepTask j= new SleepTask("j");

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
        System.out.println("===============666====");
        //---
        dag.printTopo(k);

    }

}

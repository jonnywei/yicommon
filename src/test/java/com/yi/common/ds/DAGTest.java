package com.yi.common.ds;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wjj on 6/24/17.
 */
public class DAGTest {

    @Test
    public void testGetDAG(){
        DAG<String> dag = new DAG<>();
        List<DAG.Edge> edges = new ArrayList<>();
        DAG.Edge a1= new DAG.Edge("4","9");
        DAG.Edge a2= new DAG.Edge("3","4");
        DAG.Edge a3= new DAG.Edge("7","4");
        DAG.Edge a4= new DAG.Edge("2","3");
        DAG.Edge a5= new DAG.Edge("1","2");
        edges.add(a1);
        edges.add(a2);
        edges.add(a3);
        edges.add(a4);
        edges.add(a5);

        dag.buildDAG(edges);
        dag.printTopo("9");

        List<String> topo = dag.getTopo("9");
        for(String s : topo){
            System.out.println(s);
        }
    }
}

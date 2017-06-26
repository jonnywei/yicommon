package com.yi.common.ds.v2;

import com.yi.common.scheduler.DAGTaskNode;
import com.yi.common.scheduler.v2.DAGExecNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Directed Acyclic Graph Implementation
 * Created by Jonny Wei on 2017/6/22.
 */
public class DAG<T extends Callable<V> , V> {

    public Map<T,DAGExecNode<T,V>> allNode = new HashMap<>();

    public static class Edge<T> {
        T   begin;
        T   end;

        public Edge( T  begin,  T  end) {
            this.end = end;
            this.begin = begin;
        }

        public T getBeginVertex(){
            return begin;
        }

        public  T  getEndVertex(){
            return  end;
        }
    }

    public DAG<T,V> buildDAG(List<Edge> edgeList){
        for(Edge edge: edgeList){
             addEdge(edge);
        }
        return this;
    }

    public    Node addEdge(Edge edge){
        T cobj = (T) edge.getEndVertex() ;
        DAGExecNode<T,V> child = allNode.get(cobj);
        if(! allNode.containsKey(cobj)){
            child = createNode(cobj);
            allNode.put(cobj , child);
        }

        T pobj =  (T)  edge.getBeginVertex();
        DAGExecNode parent = allNode.get(pobj);
        if(!allNode.containsKey(pobj)){
            parent = createNode(pobj);
            allNode.put(pobj, parent);
        }
        child.parents.add(parent);
        parent.successors.add(child);
        return child;
    }

    private DAGExecNode<T,V> createNode(T cobj) {
        DAGExecNode<T,V> c;
        c = new DAGExecNode(cobj.toString(),cobj);
        c.vertex = cobj;
        return c;
    }

    public DAGExecNode<T,V> addVertex(T vertex){
        if(allNode.containsKey(vertex)){
            return allNode.get(vertex);
        }
        DAGExecNode<T,V> node = createNode(vertex);
        allNode.put(vertex, node);
        return node;
    }

    public List<T> getTopo(T current){
        final  List<T> result = new ArrayList<>();
        DAGExecNode<T,V> firstNode = allNode.get(current);
        if(firstNode == null){
            return result;
        }
        dfsDAG(new VisitFn<T,V>() {
            @Override public Object apply(DAGExecNode<T,V> node) {
                result.add((T)node.vertex);
                return result;
            }
        }, firstNode);
        return result;
    }


    public  void printTopo(T current){
        DAGExecNode<T,V> firstNode = allNode.get(current);
        if(firstNode == null){
            return  ;
        }
        dfsDAG(new VisitFn<T,V>() {
            @Override public Object apply(DAGExecNode<T,V> node) {
                System.out.println(node.vertex);
                return node;
            }
        }, firstNode);
    }

    public   void visitDAG(VisitFn fn ){
        for(T key :allNode.keySet()){
            DAGExecNode<T,V> node = allNode.get(key);
            fn.apply(node);
        }
    }

    public   void dfsDAG(VisitFn fn, DAGExecNode<T,V> node){
        for(int i =0; i < node.parents.size(); i++){
            DAGExecNode<T,V> n =(DAGExecNode<T,V> )   node.parents.get(i);
            dfsDAG(fn, n);
    }
        fn.apply(node);
    }

    public DAGExecNode<T,V> getNode(T object){
        return  allNode.get(object);
    }
    public static interface VisitFn<T,V> {
        public Object apply(DAGExecNode<T, V> node);
    }

    public int vertexCount(){
       return allNode.size();
    }



    public static abstract  class Node<T,V> {
        public  final    List<DAGExecNode<T,V>> parents    = new ArrayList<>();
        public  final    List<DAGExecNode<T,V>> successors = new ArrayList<>();
        public T vertex;
    }


}

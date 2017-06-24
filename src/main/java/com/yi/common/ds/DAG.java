package com.yi.common.ds;

import java.util.*;

/**
 * Directed Acyclic Graph Implementation
 * Created by Jonny Wei on 2017/6/22.
 */
public class DAG<T> {

    public Map<T,Node<T>> allNode = new HashMap();

    public static class Node<T> {
        public  final    List<Node<T>> parents = new ArrayList<>();
        public  final    List<Node<T>> successors = new ArrayList<>();
        public T vertex;
    }

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

    public    DAG<T> buildDAG(List<Edge> edgeList){
        for(Edge edge: edgeList){
             addEdge(edge);
        }
        return this;
    }

    public    Node addEdge(Edge edge){
        T cobj = (T) edge.getEndVertex() ;
        Node<T> child = allNode.get(cobj);
        if(! allNode.containsKey(cobj)){
            child = new Node();
            child.vertex = cobj;
            allNode.put(cobj , child);
        }

        T pobj =  (T)  edge.getBeginVertex();
        Node parent = allNode.get(pobj);
        if(!allNode.containsKey(pobj)){
            parent = new Node();
            parent.vertex = pobj;
            allNode.put(pobj, parent);
        }
        child.parents.add(parent);
        parent.successors.add(child);
        return child;
    }

    public Node<T> addVertex(T vertex){
        if(allNode.containsKey(vertex)){
            return allNode.get(vertex);
        }
        Node<T> node = new Node<>();
        node.vertex = vertex;
        allNode.put(vertex, node);
        return node;
    }

    public List<T> getTopo(T current){
        final  List<T> result = new ArrayList<>();
        Node<T> firstNode = allNode.get(current);
        if(firstNode == null){
            return result;
        }
        dfsDAG(new VisitFn<T>() {
            @Override public Object apply(Node<T> node) {
                result.add(node.vertex);
                return result;
            }
        }, firstNode);
        return result;
    }


    public  void printTopo(T current){
        Node<T> firstNode = allNode.get(current);
        if(firstNode == null){
            return  ;
        }
        dfsDAG(new VisitFn<T>() {
            @Override public Object apply(Node<T> node) {
                System.out.println(node.vertex);
                return node;
            }
        }, firstNode);
    }

    public   void visitDAG(VisitFn fn ){
        for(T key :allNode.keySet()){
            Node<T> node = allNode.get(key);
            fn.apply(node);
        }
    }

    public   void dfsDAG(VisitFn fn, Node<T> node){
        for(int i =0; i < node.parents.size(); i++){
            Node<T> n =   node.parents.get(i);
            dfsDAG(fn, n);
    }
        fn.apply(node);
    }

    public Node<T> getNode(T object){
        return  allNode.get(object);
    }
    public static interface VisitFn<T> {
        public Object apply(Node<T> node);
    }

    public int vertexCount(){
       return allNode.size();
    }

}

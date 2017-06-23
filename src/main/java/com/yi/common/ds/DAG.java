package com.yi.common.ds;

import java.util.*;

/**
 * Directed Acyclic Graph Implemetion
 * Created by jianjunwei on 2017/6/22.
 */
public class DAG<T> {

    public Map<T,Node> allNode = new HashMap();

    public Node<T> firstNode = null;

    public static class Node<T> {
        final   List<Node<T>> parents = new ArrayList<>();
        final    List<Node<T>> successors = new ArrayList<>();
        T vertex;
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

    public    Node buildDAG(List<Edge> edgeList){
        for(Edge edge: edgeList){
             addEdge(edge);
        }
        return firstNode;
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
        if(firstNode ==null){
            firstNode = child;
        }
        return child;
    }


    public List<T> getTopo(){
        final  List<T> result = new ArrayList<>();
        dfsDAG(new VisitFn<T>() {
            @Override public Object apply(Node<T> node) {
                result.add(node.vertex);
                return result;
            }
        }, firstNode);
        return result;
    }


    public  void printTopo(){
        dfsDAG(new VisitFn<T>() {
            @Override public Object apply(Node<T> node) {
                System.out.println(node.vertex);
                return node;
            }
        }, firstNode);
    }

    private  void dfsDAG(VisitFn fn, Node<T> node){
        for(int i =0; i < node.parents.size(); i++){
            Node<T> n =   node.parents.get(i);
            dfsDAG(fn, n);
    }
        fn.apply(node);
    }

    private static interface VisitFn<T> {
        public Object apply(Node<T> node);
    }

    public static void main(String[] args) {
        DAG<String> dag = new DAG<>();
        List<Edge> edges = new ArrayList<>();
        Edge a1= new Edge("4","9");
        Edge a2= new Edge("3","4");
        Edge a3= new Edge("7","4");
        Edge a4= new Edge("2","3");
        Edge a5= new Edge("1","2");
        edges.add(a1);
        edges.add(a2);
        edges.add(a3);
        edges.add(a4);
        edges.add(a5);

        Node<String> current = dag.buildDAG(edges);
        dag.printTopo();

        List<String> topo = dag.getTopo();
        for(String s : topo){
            System.out.println(s);
        }

    }






}

package com.yi.common.ds;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Multimap implemetion  used linkedHashSet
 * Created by jianjunwei on 2017/6/23.
 */
public class Multimap {

    private final Map<Object,Set<Object>> fmap = new LinkedHashMap<>();

    public Set get(Object key){
        return fmap.get(key);
    }


    public Set keySet(){
        return fmap.keySet();
    }

    public void put(Object key, Object value){
        if(fmap.containsKey(key)){
            fmap.get(key).add(value);
        }else {
            Set vset = new LinkedHashSet();
            vset.add(value);
            fmap.put(key, vset);
        }
    }

    public void remove(Object key, Object value){
        if(fmap.containsKey(key)){
            fmap.get(key).remove(value);
        }
    }

    public Set removeAll(Object key){
        Set vset = fmap.get(key);
        return  vset;
    }

    @Override public String toString() {
        return super.toString();
    }
}

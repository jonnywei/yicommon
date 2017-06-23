package com.yi.common.ds;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jianjunwei on 2017/6/23.
 */
public class MultimapTest {

    @Test public  void addElemTest(){
        Multimap multimap = new Multimap();
        multimap.put("3", "r4");
        multimap.put("3", "r5");
        multimap.put("3", "r4");

        Assert.assertEquals( 1, multimap.keySet().size());
        Assert.assertEquals( 2, multimap.get("3").size());
        Assert.assertEquals(null, multimap.get("xxx"));

    }

    @Test public  void deleteElemTest(){
        Multimap multimap = new Multimap();
        multimap.put("3", "r4");
        multimap.put("3", "r5");
        multimap.put("5", "r4");

        Assert.assertEquals(2, multimap.keySet().size());

        multimap.remove("3","r4");
        Assert.assertEquals(2, multimap.keySet().size());

        Assert.assertEquals(1,multimap.get("3").size());
        Assert.assertEquals(null,multimap.get("xxx"));

    }
}

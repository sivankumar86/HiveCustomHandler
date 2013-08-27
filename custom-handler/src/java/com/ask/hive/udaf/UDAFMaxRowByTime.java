/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ask.hive.udaf;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * This is a simple UDAF that concatenates all arguments from different rows
 * into a single string.
 * 
 * It should be very easy to follow and can be used as an example for writing
 * new UDAFs.
 * 
 * Note that Hive internally uses a different mechanism (called GenericUDAF) to
 * implement built-in aggregation functions, which are harder to program but
 * more efficient.
 */
public class UDAFMaxRowByTime extends UDAF {

  /**
   * The actual class for doing the aggregation. Hive will automatically look
   * for all internal classes of the UDAF that implements UDAFEvaluator.
   */
  public static class UDAFMaxRowByTimeEvaluator implements UDAFEvaluator {

   private ArrayList<String[]> data;
   private Text prev;
   private Text current;

    public UDAFMaxRowByTimeEvaluator() {
      super();
      data = new ArrayList<String[]>();
     prev=new Text();
     current=new Text();
    }

    /**
     * Reset the state of the aggregation.
     */
    public void init() {
      data.clear();
    }

    /**
     * Iterate through one row of original data.
     * 
     * This UDF accepts arbitrary number of String arguments, so we use
     * String[]. If it only accepts a single String, then we should use a single
     * String argument.
     * 
     * This function should always return true.
     */
    public boolean iterate(String[] o) {
      if (o != null) {
       int a=data.size();
      if(a>0){
         String tmp[]=data.get(0);
      // System.out.println("sdff");
           prev.set(tmp[0]);
           current.set(o[0]);
       if(ShimLoader.getHadoopShims().compareText(prev, current) < 0){
            System.arraycopy(tmp,0,o,0,o.length);          
          o[o.length-1]="U";
          }         
         }          
       
    if(a>0)
        data.set(0,o);
     else
        data.add(o);
      }
      return true;
    }

    /**
     * Terminate a partial aggregation and return the state.
     */
    public ArrayList<String[]> terminatePartial() {
      return data;
    }

    /**
     * Merge with a partial aggregation.
     * 
     * This function should always have a single argument which has the same
     * type as the return value of terminatePartial().
     * 
     * This function should always return true.
     */
    public boolean merge(ArrayList<String[]> o) {
      if (o != null) {
int csize=data.size();
int osize=o.size();
  if(csize>0 && osize>0)
{
//String input=data.get(0);
String tmp[]=data.get(0);
//String oInput=o.get(0);	
 String tmp1[]=o.get(0);	
     //  System.out.println("merge"+input);
      //System.out.println("mergeO"+oInput);
           prev.set(tmp[0]);
           current.set(tmp1[0]);
int compare=ShimLoader.getHadoopShims().compareText(prev, current);
       if(compare < 0){
            System.arraycopy(tmp1,0,tmp,0,tmp.length);
              tmp[tmp.length-1]="U";
          } 
      else if(compare ==0){
           tmp[tmp.length-1]="I";
         } else{
           tmp[tmp.length-1]="U";
        }      
      data.set(0,tmp);    

}else{
 data.add(o.get(0));
}
        //data.addAll(o);
      }
      return true;
    }

    /**
     * Terminates the aggregation and return the final result.
     */
    public String terminate() {
     // Collections.sort(data);
    // System.out.println("Size"+data.size());
      String s="";
if(data.size()>0)
StringUtils.join(data.get(0),'\t');
      return s;
    }

  }

}

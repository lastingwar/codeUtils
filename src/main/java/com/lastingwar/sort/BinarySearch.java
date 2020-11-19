package com.lastingwar.sort;



/**
 * 二分查找 时间复杂度O(log2n);空间复杂度O(1)
 * @author yhm
 * @create 2020-11-18 21:10
 */
public class BinarySearch {
    public static int binarySearch(int[] data,int left,int right,int point){
        if (left > right){
            return -1;
        }
        int mid = (right + left) / 2;

        int result = data[mid];
        if (result < point){
            result = binarySearch(data,mid,right,point);
        }else if (result > point){
            result = binarySearch(data,left,mid,point);
        }
        return result;
    }
}

package com.lastingwar.sort;

import java.util.HashMap;

/**
 * 快排
 * 时间复杂度:O(nlogn)
 * 空间复杂度:O(n)
 * @author yhm
 * @create 2020-11-19 10:25
 */
public class MergeSort {
    public static void mergeSort(DataWrap[] data) {
        // 归并排序
        sort(data, 0, data.length - 1);
    }

    // 将索引从left到right范围的数组元素进行归并排序
    private static void sort(DataWrap[] data, int left, int right) {
        if(left < right){
            //找出中间索引
            int center = (left + right)/2;
            sort(data,left,center);
            sort(data,center+1,right);
            //合并
            merge(data,left,center,right);
        }
    }

    // 将两个数组进行归并，归并前两个数组已经有序，归并后依然有序
    private static void merge(DataWrap[] data, int left, int center, int right) {
        DataWrap[] tempArr = new DataWrap[data.length];
        int mid = center + 1;
        int third = left;
        int temp = left;
        //填充新数组,比较左右数组的最小值
        while (left <= center && mid <= right) {
            if (data[left].compareTo(data[mid]) <= 0) {
                tempArr[third++] = data[left++];
            } else {
                tempArr[third++] = data[mid++];
            }
        }
        //填充新数组,使用左右数组剩下的值
        while (mid <= right) {
            tempArr[third++] = data[mid++];
        }
        while (left <= center) {
            tempArr[third++] = data[left++];
        }
        //使用新数组覆盖原数组
        while (temp <= right) {
            data[temp] = tempArr[temp++];
        }
    }

    public static void main(String[] args) {
        HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();

        stringIntegerHashMap.put("hello",1);
    }
}

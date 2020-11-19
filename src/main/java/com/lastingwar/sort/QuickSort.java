package com.lastingwar.sort;

import java.util.Arrays;

/**
 * 快排
 *  * 时间复杂度:平均时间复杂度为O(nlogn)
 *  * 空间复杂度:O(logn)，因为递归栈空间的使用问题
 * @author yhm
 * @create 2020-11-18 21:18
 */
public class QuickSort {
    private static void swap(DataWrap[] data, int i, int j) {
        DataWrap temp = data[i];
        data[i] = data[j];
        data[j] = temp;
    }
    private static void swap(int[] data, int i, int j) {
        int temp = data[i];
        data[i] = data[j];
        data[j] = temp;
    }

    private static void subSort(int[] data ,int start ,int end){
        if (start < end){
            int base = data[start];
            // 设置左右守卫
            int left = start;
            int right = end + 1;
            while (true){
                // 当左下标小于最右边时,逐个右移,直到数值大于基准
                while (left < end && data[++left] <= base){}
                // 当右下标小于最左边时,逐个左移,直到数值小于基准
                while (right > start && data[--right] >= base){}
                // 移动完之后,交换左右下标的数据,重复上动作
                if (left < right){
                    swap(data,left,right);
                }else {
                    break;
                }
            }
            // right为找到的新的基准,切分为两个数组排序
            swap(data,start,right);
            subSort(data, start, right - 1);
            subSort(data, right + 1, end);
        }
    }
    private static void subSort(DataWrap[] data, int start, int end) {
        if (start < end) {
            DataWrap base = data[start];
            int left = start;
            int right = end + 1;
            while (true) {
                while (left < end && data[++left].compareTo(base) <= 0) {
                }
                while (right > start && data[--right].compareTo(base) >= 0) {
                }
                if (left < right) {
                    swap(data, left, right);
                } else {
                    break;
                }
            }
            swap(data, start, right);
            subSort(data, start, right - 1);
            subSort(data, right + 1, end);
        }
    }
    public static void quickSort(DataWrap[] data){
        subSort(data,0,data.length-1);
    }
    public static void quickSort(int[] data){
        subSort(data,0,data.length-1);
    }

    public static void main(String[] args) {
        DataWrap[] data = { new DataWrap(9, ""), new DataWrap(-16, ""),
                new DataWrap(21, "*"), new DataWrap(23, ""),
                new DataWrap(-30, ""), new DataWrap(-49, ""),
                new DataWrap(21, ""), new DataWrap(30, "*"),
                new DataWrap(30, "") };
        System.out.println("排序之前：\n" + java.util.Arrays.toString(data));
        quickSort(data);
        System.out.println("排序之后：\n" + java.util.Arrays.toString(data));
        int[] ints = {15,7,20,10,8,16};
        quickSort(ints);
        System.out.println(Arrays.toString(ints));
    }
}

package com.lastingwar.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author yhm
 * @create 2020-11-16 20:39
 */
public class HbaseClient {

    public static Connection connection ;

    // 获取连接
    static{
        try {
            Configuration configuration = HBaseConfiguration.create();
            //设置zk的地址
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *  过滤查询数据
     * @throws IOException
     */
    public static void scanDataWithCondition() throws IOException {
        String namespace  = "lastingwar";
        String tablename  = "student";
        String cf = "info";
        String cl = "sex" ;

        Table table =
                connection.getTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));
        Scan scan = new Scan();
        //需求一: 查rowkey中包含字母"2"的数据
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL,new SubstringComparator("2"));

        //需求二: 查sex为 'male'的数据
        SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(cf),
                Bytes.toBytes(cl),
                CompareOperator.EQUAL,
                Bytes.toBytes("female"));
        columnValueFilter.setFilterIfMissing(true);

        //需求三:  需求一 and 需求二

               List<Filter> filters = new ArrayList<>();
        filters.add(rowFilter);
        filters.add(columnValueFilter);

        FilterList filterLists = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);

        scan.setFilter(filterLists);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+":"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) +":" +
                                Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        scanner.close();
        table.close();
    }


    /**
     * 扫描表数据
     * @param namespace
     * @param tablename
     * @param startrow 开始rowkey
     * @param stoprow 结束rowkey
     * @throws IOException
     */
    public static void scanData(String namespace,String tablename ,String startrow,String stoprow) throws IOException {
        if(!existTable(namespace,tablename)){
            System.out.println(namespace + ":" + tablename + " 不存在");
            return ;
        }
        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startrow)).withStopRow(Bytes.toBytes(stoprow));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+":"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) +":" +
                                Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        scanner.close();
        table.close();
    }


    /**
     * 打印列数据
     * @param namespace
     * @param tablename
     * @param rowkey
     * @param cf
     * @param cl
     * @throws IOException
     */
    public static void getData(String namespace,String tablename,String rowkey,String cf,String cl) throws IOException {
        if(!existTable(namespace,tablename)){
            System.out.println(namespace + ":" + tablename + " 不存在");
            return ;
        }
        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl));
            Result result = table.get(get);
            //获取所有的cell,每个cell实际上就是物理存储中的一行中某个列的数据
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+":"+
                        Bytes.toString(CellUtil.cloneFamily(cell))+":" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) +":" +
                        Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }finally {
            table.close();
        }
    }


    /**
     * 删除数据
     * @param namespace 命名空间
     * @param tablename 表名
     * @param rowkey rowkey
     * @param cf 列族
     * @param cl 列
     * @throws IOException
     */
    public static void deleteData(String namespace,String tablename, String rowkey ,String cf, String cl ) throws IOException {
        if(!existTable(namespace,tablename)){
            System.out.println(namespace + ":" + tablename + " 不存在");
            return ;
        }
        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cl));
            table.delete(delete);
        }finally {
            table.close();
        }
    }

    /**
     * 添加表数据
     * @param namespace 命名空间
     * @param tablename 表名
     * @param rowkey rowkey
     * @param cf 列族
     * @param cl 列
     * @param value 值
     * @throws IOException
     */
    public static void putData(String namespace,String tablename ,String rowkey , String cf ,String cl ,String value ) throws IOException {
        if(!existTable(namespace,tablename)){
            System.out.println(namespace + ":" + tablename + " 不存在");
            return ;
        }
        //获取table对象
        Table table =
                connection.getTable(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)));
        try {
            //创建put对象
            Put put = new Put(Bytes.toBytes(rowkey));
            //给put对象设置列族, 列, value
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),Bytes.toBytes(value));
            table.put(put);
        }finally {
            table.close();
        }
    }



    /**
     * 扩展:
     *    admin.modifyTable();
     *    admin.listTableDescriptors();
     */

    /**
     * 删除表
     * @param namespace
     * @param tablename
     * @throws IOException
     */
    public static void deleteTable(String namespace,String tablename) throws IOException {
        if(!existTable(namespace,tablename)){
            System.out.println(namespace +":" + tablename + " 不存在");
            return ;
        }
        Admin admin  = connection.getAdmin();
        //禁用表
        admin.disableTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));
        //删除表
        admin.deleteTable(TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(tablename)));

        admin.close();
    }

    /**
     * 判断表是否存在
     * @param namespace
     * @param tablename
     * @return
     * @throws IOException
     */
    public static boolean  existTable(String namespace ,String tablename) throws IOException {
        Admin admin = connection.getAdmin();
        if(admin.tableExists(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)))){
            admin.close();
            return true ;
        }
        admin.close();
        return false ;
    }


    /**
     * 创建表
     * @param namespace 命名空间
     * @param tablename 表名
     * @param cfs 列族 每个字符串为一个列族
     * @throws IOException
     */
    public static void createTableNew(String namespace,String tablename,String ... cfs) throws IOException {
        // 基本的参数判断
        if(tablename== null || cfs == null || cfs.length <=0){
            System.out.println("createTable 参数有误!!!!");
            return ;
        }
        Admin admin = connection.getAdmin();
        try {
            //判断表是否存在
            if(admin.tableExists(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)))){
                System.out.println(namespace +":"+tablename + "表已经存在");
                return ;
            }
            //建表
            TableDescriptorBuilder  descriptorBuilder =
                        TableDescriptorBuilder.newBuilder(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)));
            //列族
            for (String cf : cfs) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                descriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            //获取TableDescriptor对象
            TableDescriptor tableDescriptor = descriptorBuilder.build();
            admin.createTable(tableDescriptor);
        }finally{
            admin.close();
        }
    }



    @Deprecated
    public static void createTable(String namespace,String tablename,String ... cfs) throws IOException {
            // 基本的参数判断
            if(tablename== null || cfs == null || cfs.length <=0){
                System.out.println("createTable 参数有误!!!!");
                return ;
            }
            Admin admin = connection.getAdmin();
        try {
            //判断表是否存在
            boolean exists ;
            if(namespace != null){
                exists = admin.tableExists(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)));
            }else{
                exists = admin.tableExists(TableName.valueOf(Bytes.toBytes(tablename)));
            }

            if(exists){
                System.out.println(tablename + "表已经存在");
                admin.close();
                return ;
            }
            //建表
            TableDescriptorBuilder  descriptorBuilder;
            if(namespace != null){
                descriptorBuilder =
                        TableDescriptorBuilder.newBuilder(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tablename)));
            }else{
                descriptorBuilder =
                        TableDescriptorBuilder.newBuilder(TableName.valueOf(Bytes.toBytes(tablename)));
            }
            //列族
            for (String cf : cfs) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                descriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            //获取TableDescriptor对象
            TableDescriptor tableDescriptor = descriptorBuilder.build();

            admin.createTable(tableDescriptor);
        }finally{
            admin.close();
        }
    }


    /**
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
            NamespaceDescriptor namespaceDescriptor = builder.build();
            //创建
            admin.createNamespace(namespaceDescriptor);


        }catch (NamespaceExistException e){
            System.out.println(namespace + " 已经存在");
        }finally {
            admin.close();
        }
    }

    public static void main(String[] args) throws IOException {
        //createNamespace("lastingwar");
        //createTable(null,"test","info");
        createTableNew("lastingwar","student1","info");
        //deleteTable("lastingwar","student1");
        //putData("lastingwar","student","1001","info","name","zhangsan");
        //deleteData("lastingwar","student","1001","info","name");

        //getData("lastingwar","student","1002","info","name");

        //scanData("lastingwar","student","1001","1003");
        scanDataWithCondition();
    }
}

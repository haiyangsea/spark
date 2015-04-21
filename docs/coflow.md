添加Coflow Shuffle时遇到的问题总结：
---

1 遇到Shuffle文件不能打开错误
   
    在注册Shuffle数据的时候，需要先调用Shuffle Writer Handler的stop方法，然后再注册Coflow流，最后将stop方法的返回值返回
   
2 在读取Shuffle数据的时候，Varys Master UI上显示只有一个机器上的流变成了Active

    在对Coflow进行测试的时候，不能使用take这样的操作，而需要collect，count这样的Action，take好像一次只会扫描一个Partition

3 运行时报java.io.IOException: failed to read chunk

    当在解压缩的时候，传入的inputStream() ByteBuffer构造而来，并且ByteBuffer是堆外的，这样通过blockManager.wrapForCompression
    方法得到的Iterator，如果是在当前方法中使用不会报错，但是如果到其他方法中使用，就会报该错，可以将ByteBuffer中堆外的数据读到堆内来。
  
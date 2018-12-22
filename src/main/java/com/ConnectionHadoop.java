package com;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class ConnectionHadoop {
	public static void main(String[] args) {
		PrivilegedExceptionAction<Void> pea = new PrivilegedExceptionAction<Void>() {

			@Override
			public Void run() throws Exception {
				// 하둡 접속하는 방식
				Configuration config = new Configuration();
				config.set("fs.defaultFS", "hdfs://192.168.0.157:9000/user/bdi");
				config.setBoolean("dfs.support.append", true);
				
				FileSystem fs = FileSystem.get(config);
				
				Path upFileName = new Path("word.txt");
				if(fs.exists(upFileName)) { // word.txt 파일이 있다면
					fs.delete(upFileName,true); // 지워버리고
				}
				FSDataOutputStream fsdo = fs.create(upFileName); // 다시 만든다
				fsdo.writeUTF("hi hi hi hey hey hi start hi");  // 그안에 이 내용을 쓰고
				fsdo.close();	// 닫는다
				
				Path dirName = new Path("/user/bdi"); // /user/bdi안에 뭐가 있는지 찾는다.
				FileStatus[] files = fs.listStatus(dirName);
				for(FileStatus file:files) {
					System.out.println(file);
				}
				return null;
			}
		};
		
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bdi");
		try {
			ugi.doAs(pea);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

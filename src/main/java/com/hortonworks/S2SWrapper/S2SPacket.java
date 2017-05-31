package com.hortonworks.S2SWrapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.remote.protocol.DataPacket;

public class S2SPacket implements DataPacket{
    
	private final InputStream input;
	private final File file;
	private final long size;
	private final Map<String,String> attributes;
	
	public S2SPacket(final File file, final Map<String,String> attributes) throws IOException{
		if(file!=null){
			this.file = file;
			this.size = file.length();
			this.input= new FileInputStream(file);
		
		}else{
			this.file=null;
			this.size=0;
			this.input = new ByteArrayInputStream(new byte[0]);
		}
		if(attributes==null)
			this.attributes = new HashMap<String,String>();
		else
			this.attributes=attributes;
	}
	public S2SPacket(final String data, final Map<String,String> attributes) throws IOException{
		this.file=null;
		if(data!=null){
			this.size = data.length();
			this.input= new ByteArrayInputStream(data.getBytes());
		}else{
			this.size=0;
			this.input = new ByteArrayInputStream(new byte[0]);
		}
		
		if(attributes==null)
			this.attributes = new HashMap<String,String>();
		else
			this.attributes=attributes;
	}
	
	public Map<String, String> getAttributes() {
		// TODO Auto-generated method stub
		return this.attributes;
	}

	public InputStream getData() {
		// TODO Auto-generated method stub
		return this.input;
	}

	public long getSize() {
		// TODO Auto-generated method stub
		return this.size;
	}
     
}

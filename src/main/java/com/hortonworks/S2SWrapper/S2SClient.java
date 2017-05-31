package com.hortonworks.S2SWrapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;

/**
 * Hello world!
 *
 */
public class S2SClient {
	
	private final SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder();
	
	public S2SClient(){
		//builder= new SiteToSiteClient.Builder();
	}
	
	public S2SClient(String url,String portName,boolean useRaw){
		builder.url(url);
		builder.portName(portName);
		if(useRaw)
			builder.transportProtocol(SiteToSiteTransportProtocol.RAW);
		else
			builder.transportProtocol(SiteToSiteTransportProtocol.HTTP);
		
	}
	
	public long sendString(String data,Map<String,String> attributes) throws IOException{
		S2SPacket packet = new S2SPacket(data, attributes);
		return send(packet,attributes);
	}
	
	public long sendFiles(File file,Map<String,String> attributes) throws IOException{
		S2SPacket packet = new S2SPacket(file, attributes);
		return send(packet,attributes);
	}
	
	private long send(S2SPacket packet,Map<String,String>attributes)throws IOException{
		builder.nodePenalizationPeriod(60, TimeUnit.SECONDS);
		SiteToSiteClient client = builder.build();
		Transaction transaction = client.createTransaction(TransferDirection.SEND);
        transaction.send(packet);
        transaction.confirm();
        TransactionCompletion completion =  transaction.complete();
        return completion.getBytesTransferred();
	}
	
    public static void main( String[] args )
    {
    	S2SClient client = new S2SClient("http://localhost:9988/nifi","javainput",false);
    	HashMap<String,String> attr = new HashMap<String,String>();
    	attr.put("whoami", "java client");
    	try{
    	long bytes = client.sendString("Hello from java", attr);
    	System.out.println("byets transferred" + bytes);
    	}catch(Exception e){
    		e.printStackTrace(System.out);
    	}
        
    }
}

package quidgest.solr.dataimport;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.Transformer;

public class HashFilter extends Transformer {
	
	@Override
	public Object transformRow(Map<String, Object> row, Context arg1) {
		
		String body = ((String) row.get("text"));
		String res=calcularHash(body);
		row.put("hash", res);
		
		return row;
	}

	private static String calcularHash(String body) {
		 try {
			MessageDigest md = MessageDigest.getInstance("SHA");
			byte[] hash = new byte[0];
			if(body!=null) {
				hash = md.digest(body.getBytes());
			}
			
			StringBuilder sb = new StringBuilder();
		    for (byte b : hash) {
		        sb.append(String.format("%02X", b));
		    }
		    
			return sb.toString();
		 } catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 }
		 
		 return "";
	}
}

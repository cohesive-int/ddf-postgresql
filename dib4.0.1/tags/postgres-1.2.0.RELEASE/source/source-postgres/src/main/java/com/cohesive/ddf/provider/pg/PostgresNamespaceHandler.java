package com.cohesive.ddf.provider.pg;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.ext.XLogger;

public class PostgresNamespaceHandler {
    
    private static final XLogger logger = new XLogger( LoggerFactory.getLogger(PostgresNamespaceHandler.class ) );
    
    private static final String FELIX_FILENAME = "felix.fileinstall.filename";
    private static final String SERVICE_PID = "service.pid";

    private Map<String,String> namespaceMap = null;
    private String namespaceString = null;
    
    public PostgresNamespaceHandler(){
    	namespaceMap = new HashMap<String,String>();
    	loadDefaultNamespaces();
    }
    
	public void updateNamespaces( Map<String,String> updatedMap ){

    	int size = updatedMap.size();
    	logger.debug( "Updating namespace values with " + size + " values" );
    	if ( size > 0 ){
    		namespaceMap = updatedMap;	
	    	for (String key : updatedMap.keySet()) {
	    		logger.debug( "Added namespace mapping ['" + key +"', '" + updatedMap.get( key ) +"']" );
			}
    	}else{
    		loadDefaultNamespaces();
    	}
        namespaceString = convertNamespacesAsString();
    }

	public String getNamespacesAsString(){
        return namespaceString;
    }
    
    public Map<String,String> getNamespaceMap(){
        return namespaceMap;
    }

    // load the default namespaces
    protected void loadDefaultNamespaces(){
    	namespaceMap.clear();
        /*namespaceMap.put( "default", "http://dcgs.mil/metadata"  );
        namespaceMap.put( "metadata", "http://dcgs.mil/metadata" );
        namespaceMap.put( "dcgs", "http://dcgs.mil/metadata" );
        namespaceMap.put( "dcgsaf", "http://dcgs.mil/metadata" );
        namespaceMap.put( "isr", "urn:us:gov:cryp:ddms:isr" );
        namespaceMap.put( "ddms", "http://metadata.dod.mil/mdr/ns/DDMS/2.0/" );
        namespaceMap.put( "ICISM", "urn:us:gov:ic:ism:v2" );
        namespaceMap.put( "gml", "http://www.opengis.net/gml" );
        namespaceMap.put( "asa", "http://dcgs.mil/metadata/asa" );
        namespaceMap.put( "tns", "http://dcgs.mil/metadata/asa" );
        namespaceMap.put( "army", "http://dcgs.mil/metadata/army" );
        namespaceMap.put( "psds", "http://psds.mil/metadata" );
        namespaceMap.put( "ipb", "http://dcgs.mil/metadata/ipb" );
        namespaceMap.put( "mtf", "urn:mtf:mil:usmtf:2000" );
        namespaceMap.put( "s", "urn:mtf:mil:usmtf:2000" );*/
    }
    
    public String convertNamespacesAsString() {
        StringBuilder sb = new StringBuilder( "ARRAY[" );

        for ( String prefix : namespaceMap.keySet() ) {
        	if ( !SERVICE_PID.equals(prefix) && !FELIX_FILENAME.equals( prefix )){
	            sb.append( "ARRAY['" );
	            sb.append( prefix == null || prefix.equalsIgnoreCase( "default" ) ? "default" : prefix );
	            sb.append( "', '" );
	            sb.append( namespaceMap.get( prefix ) );
	            sb.append( "']," );
        	}
        }
        sb.deleteCharAt( sb.length() - 1 );
        sb.append( "]" );
        return sb.toString();
    }
    
}

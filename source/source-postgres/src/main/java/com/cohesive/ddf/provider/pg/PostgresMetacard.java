package com.cohesive.ddf.provider.pg;

import java.net.URI;
import java.util.Date;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.MetacardImpl;

public class PostgresMetacard extends MetacardImpl{
	
public static final String NO_RESOURCE_URI_SCHEME = "NoResource://";
    
    //private static final String EMPTY_RESOURCE_FIELD = "N/A";
	//private static final String UNKNOWN_CONTENT = "Unknown";
	private static final long serialVersionUID = 1L;
	private static final URI EMPTY_DAD_URI= URI.create( "dad:///N%2FA?N%2FA#N%2FA");
	//private static final URI EMPTY_MALFORMED_DAD_URI= URI.create( "dad:///N%2FA?N%2FA#%3F");
	
	public PostgresMetacard( Metacard metacard, String sourceId ){
		super( metacard );
		setSourceId( sourceId );
	}
	
	public String getProviderResourceURI(){
	    URI uri = getResourceURI();
	    //return uri == null || EMPTY_DAD_URI.equals( uri ) ||  EMPTY_MALFORMED_DAD_URI.equals( uri ) ? NO_RESOURCE_URI_SCHEME + getId() : uri.toASCIIString(); 
	    return uri == null || EMPTY_DAD_URI.equals( uri ) ? NO_RESOURCE_URI_SCHEME + getId() : uri.toASCIIString();
	}

}

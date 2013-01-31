package com.cohesive.ddf.provider.common;

import java.net.URI;
import java.util.Date;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.MetacardImpl;


public class ProviderMetacard extends MetacardImpl{

    public static final String NO_RESOURCE_URI_SCHEME = "NoResource://";
    
    //private static final String EMPTY_RESOURCE_FIELD = "N/A";
	private static final String UNKNOWN_CONTENT = "Unknown";
	private static final long serialVersionUID = 1L;
	private static final URI EMPTY_DAD_URI= URI.create( "dad:///N%2FA?N%2FA#N%2FA");
	//private static final URI EMPTY_MALFORMED_DAD_URI= URI.create( "dad:///N%2FA?N%2FA#%3F");
	

    public ProviderMetacard( Metacard metacard, Date now, String id, String sourceId ){
	    super( metacard );
	    setId( id );
        initValues( now, sourceId );
	}
	
	public ProviderMetacard( Metacard metacard, Date now, String sourceId ){
        super( metacard );
        initValues( now, sourceId );
    }
	
	@Override
	public URI getResourceURI(){
	    URI uri = super.getResourceURI();
	    //return uri == null || EMPTY_DAD_URI.equals( uri ) ||  EMPTY_MALFORMED_DAD_URI.equals( uri ) ? NO_RESOURCE_URI_SCHEME + getId() : uri.toASCIIString(); 
	    return uri == null || EMPTY_DAD_URI.equals( uri ) ? null : uri;
	}
	
	public String getProviderResourceURI(){
	    URI uri = getResourceURI();
	    //return uri == null || EMPTY_DAD_URI.equals( uri ) ||  EMPTY_MALFORMED_DAD_URI.equals( uri ) ? NO_RESOURCE_URI_SCHEME + getId() : uri.toASCIIString(); 
	    return uri == null ? null : uri.toASCIIString();
	}
	
	@Override
	public String getResourceSize(){
	    String size = super.getResourceSize();
	    //return size == null || size.trim().isEmpty() || EMPTY_RESOURCE_FIELD.equals( size ) ? null : size ;
	    return size == null || size.trim().isEmpty() ? null : size ;
	}
	
	//private String generatePrimaryKey(){
	//	return UUID.randomUUID().toString().replaceAll("-", "");
	//}	
	
	private void initValues( Date now, String sourceId ) {
        
        if ( getEffectiveDate() == null ){
            setEffectiveDate( now );
            setModifiedDate( now );
            setCreatedDate( now );
        }
        String value = getTitle();
        if ( value == null || value.trim().isEmpty() ){
            setTitle( UNKNOWN_CONTENT );
        }
        value = getContentTypeName();
        if ( value == null || value.trim().isEmpty() ){
            setContentTypeName( UNKNOWN_CONTENT );
        }
        
        value = getContentTypeVersion();
        if ( value == null || value.trim().isEmpty() ){
            setContentTypeVersion( UNKNOWN_CONTENT );
        }
        
        setSourceId( sourceId );
        
    }

}

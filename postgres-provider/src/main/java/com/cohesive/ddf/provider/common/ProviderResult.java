package com.cohesive.ddf.provider.common;

import java.net.URI;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.MetacardImpl;
import ddf.catalog.data.Result;



public class ProviderResult extends MetacardImpl implements Result{

    private static final long serialVersionUID = 1L;
    private Double relevance = null;
    private Double distance = null;
    
    public ProviderResult(){
    }

    @Override
    public Metacard getMetacard() {
        return this;
    }

    @Override
    public Double getRelevanceScore() {
        return relevance;
    }
    
    public void setRelevance( Double r ){
        relevance = r;
    }
    
    @Override
    public void setThumbnail( byte[] bytes ) {
        if ( bytes != null & bytes.length == 0 )
            super.setThumbnail( null );
        else {
            super.setThumbnail( bytes );
        }
    }
    

    @Override
    public Double getDistanceInMeters() {
        return distance;
    }
    
    public void setDistance( Double d ){
        distance = d;
    }
    
    public void setResourceUri( String uri ){
        if ( !uri.startsWith( ProviderMetacard.NO_RESOURCE_URI_SCHEME ) ){
            super.setResourceURI( URI.create( uri ) );
        }
    }
    
    

}

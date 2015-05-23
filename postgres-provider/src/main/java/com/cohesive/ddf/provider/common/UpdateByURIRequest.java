package com.cohesive.ddf.provider.common;

import ddf.catalog.data.Metacard;



public class UpdateByURIRequest {
    
    private Metacard metacard = null;
    private String uri = null;
    
    public UpdateByURIRequest( Metacard metacard, String uri ){
        this.metacard = metacard;
        this.uri = uri;
    }
    
    public Metacard getMetacard(){
        return metacard;
    }
    
    public String getUpdateByURIValue(){
        return uri;
    }

}

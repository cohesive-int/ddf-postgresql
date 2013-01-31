package com.cohesive.ddf.provider.persistence;

import java.util.List;

import com.cohesive.ddf.provider.common.query.CatalogQuery;

import ddf.catalog.data.Result;

public interface QueryMapper {
	
    public List<Result> query( CatalogQuery query );
    public int getTotalCount( CatalogQuery query );
    public int testConnection();
}

package com.cohesive.ddf.provider.persistence;

import java.net.URI;
import java.util.List;

import com.cohesive.ddf.provider.common.query.CatalogQuery;

import ddf.catalog.data.Result;

public interface QueryMapper {

    public List<Result> query( CatalogQuery query );

    public List<Result> queryById( List<String> ids );

    public List<Result> queryByURI( List<URI> uris );

    public int getTotalCount( CatalogQuery query );

    public int testConnection();
}

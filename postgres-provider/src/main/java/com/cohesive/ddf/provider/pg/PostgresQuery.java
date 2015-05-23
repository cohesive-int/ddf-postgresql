/**
 *  Copyright Cohesive Integrations - 2011
 */
package com.cohesive.ddf.provider.pg;

import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cohesive.ddf.provider.common.query.CatalogQuery;

import ddf.catalog.data.Result;
import ddf.catalog.operation.Query;
import ddf.catalog.source.UnsupportedQueryException;

/**
 * @author Nguessan Kouame
 * @author Jeff Vettraino
 * 
 */
public class PostgresQuery implements CatalogQuery {

private static final Logger logger = LoggerFactory.getLogger(PostgresQuery.class );
	
    private static final long serialVersionUID = 1L;
    protected static final String DEFAULT_NAMESPACE = "default:";
    protected static final char DIB_WILDCARD_CHAR = '*';
    protected static final char RDBMS_WILDCARD_CHAR = '%';
    protected static final String DEFAULT_SORT_COLUMN = "CATALOG_EFFECTIVE_TIMESTAMP";
    protected static final String SORT_ORDER_ASC = "ASC";
    protected static final String SORT_ORDER_DESC = "DESC";

    protected String sourceId = null;

    protected String sortOrder = null;
    protected boolean relevanceSort = false;
    protected boolean distanceSort = false;
    protected int maxResults;
    protected int offset;
    protected SQLHolder holder;
    
    
    public PostgresQuery( Query query, SQLHolder holder, String sourceId ){
        this.holder = holder;
        this.sourceId = sourceId;
        Integer tempInt = query.getPageSize();
        maxResults = tempInt == null ? 1000 : tempInt.intValue();
        
        tempInt = query.getStartIndex();
        offset = ( tempInt == null || tempInt <=0 ) ? 0 : tempInt.intValue() - 1;
        
        /* Sorting */
        SortBy sortBy = query.getSortBy();
        SortOrder orderType = null;
        
        if (sortBy != null) {
            orderType = sortBy.getSortOrder();
            String sortProperty = sortBy.getPropertyName().getPropertyName();
            if ( sortProperty != null ){
                if ( sortProperty.equals( Result.RELEVANCE ) && holder.isAddRelevance() ){
                    relevanceSort = true;
                    sortOrder = Result.RELEVANCE + " " + SORT_ORDER_DESC ;
                }else if ( sortProperty.equals( Result.DISTANCE ) && holder.isAddDistance() ) {
                    distanceSort = true;
                    sortOrder = Result.DISTANCE + " " + ( SortOrder.DESCENDING.equals( orderType ) ? SORT_ORDER_DESC : SORT_ORDER_ASC );
                }
            }
        }
        if ( sortOrder == null ){
            sortOrder = DEFAULT_SORT_COLUMN + " " + ( SortOrder.ASCENDING.equals( orderType ) ? SORT_ORDER_ASC : SORT_ORDER_DESC );
        }
    }
    
    public void parseQuery( Query origQuery, String sourceId) throws UnsupportedQueryException{
        this.sourceId = sourceId;   
    }
    
    @Override
    public String getSourceId(){
        return sourceId;
    }


    @Override
    public int getMaxResults(){
        return maxResults;
    }
    
    @Override
    public int getOffset(){
        return offset;
    }

    
    @Override
    public String getOrderBy(){
        return sortOrder;
    }
    
    @Override
    public boolean isRelevanceSort(){
        return relevanceSort;
    }

    @Override
    public boolean isDistanceSort(){
        return distanceSort;
    }

    @Override
    public String getQuery() {
    	logger.debug( "SQL Where clause: " + holder.getQuery() );
        return holder.getQuery();
    }

    @Override
    public String getAdditionalSelectStmts() {
        // TODO Auto-generated method stub
        return holder.getFromStmts();
    }
    
    public String getAdditionalFromStmts(){
        return holder.getFromStmts();
    }
    
    public boolean isAddRelevance(){
        return holder.isAddRelevance();
    }
    
    public boolean isAddDistance(){
        return holder.isAddDistance();
    }
    
    public String getWkt(){
        return holder.getWkt();
    }
    
}

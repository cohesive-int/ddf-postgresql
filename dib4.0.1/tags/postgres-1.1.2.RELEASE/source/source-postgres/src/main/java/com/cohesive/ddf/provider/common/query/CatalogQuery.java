package com.cohesive.ddf.provider.common.query;

import java.io.Serializable;

public interface CatalogQuery extends Serializable {
    
    public String getQuery();
    
    public String getAdditionalSelectStmts();
    
    public String getAdditionalFromStmts();
    
    public String getSourceId();
    
    public int getMaxResults();
    
    public int getOffset();
    
    public String getOrderBy();
    
    public boolean isRelevanceSort();

    public boolean isDistanceSort();

}

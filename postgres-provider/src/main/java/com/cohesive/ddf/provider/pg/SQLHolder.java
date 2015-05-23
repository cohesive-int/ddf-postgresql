package com.cohesive.ddf.provider.pg;


public class SQLHolder {

    private StringBuilder queryBuilder = new StringBuilder();
    private String fromStmts = null;
    private boolean addRelevance = false;
    private boolean addDistance = false;
    private String wkt = null;
    
    public SQLHolder( String queryClause ){
        this.queryBuilder.append( queryClause );
    }
    
    public SQLHolder( String queryClause, SQLHolder oldHolder ){
        this.queryBuilder.append( queryClause );
        fromStmts = oldHolder.getFromStmts();
        addRelevance = oldHolder.isAddRelevance();
        addDistance = oldHolder.isAddDistance();
        wkt = oldHolder.getWkt();
    }
    
    public String getQuery(){
        return queryBuilder.toString();
    }
    
    public void setQuery( String query ){
        queryBuilder = new StringBuilder( query );
    }
    
    public void append( String operand ){
        queryBuilder.append( operand );
    }
    
    public String getFromStmts(){
        return fromStmts;
    }
    
    public void addFromStmt( String tsQuery ){
        fromStmts = ( fromStmts == null ? tsQuery : fromStmts + " " + tsQuery );
    }
    
    public String getWkt(){
        return wkt;
    }
    
    public void setWkt( String wkt ){
        this.wkt = wkt;
    }
    
    public boolean isAddRelevance(){
        return addRelevance;
    }
    
    public void setAddRelevance( boolean rel ){
        addRelevance = rel;
    }
    
    public boolean isAddDistance(){
        return addDistance;
    }
    
    public void setAddDistance( boolean dist ){
        addDistance = dist;
    }
}

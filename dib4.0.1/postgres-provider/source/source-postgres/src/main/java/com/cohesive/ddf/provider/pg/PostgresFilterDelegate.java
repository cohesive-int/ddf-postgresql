package com.cohesive.ddf.provider.pg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import org.slf4j.ext.XLogger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import ddf.catalog.data.Metacard;
import ddf.catalog.filter.FilterDelegate;

/**
 * Translates filter-proxy calls into PostgreSQL query syntax.
 * 
 * @author info@cohesiveintegrations.com 
 */
public class PostgresFilterDelegate extends FilterDelegate<SQLHolder> {

    private static final XLogger logger = new XLogger( LoggerFactory.getLogger(PostgresCatalogProvider.class ) );
    private static final String DEFAULT_NAMESPACE = "default:";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    
    private static double DEGREES_CONVERSTION_DEFAULT = 80000;
    private static double NEAREST_NEIGHBOR_RADIUS = 500000;
    
    private static final String CATALOG_METADATA  = "CATALOG_METADATA";
    private static final String CATALOG_SOURCE_LOCATION = "CATALOG_SOURCE_LOCATION";
    private static final String CATALOG_TITLE = "CATALOG_TITLE";
    private static final String CATALOG_EFFECTIVE_DATE = "CATALOG_EFFECTIVE_TIMESTAMP";
    private static final String CATALOG_EXPIRATION_DATE = "CATALOG_EXPIRATION_TIMESTAMP";
    private static final String CATALOG_MODIFIED_DATE = "CATALOG_MODIFIED_TIMESTAMP";
    private static final String CATALOG_CREATED_DATE = "CATALOG_CREATED_TIMESTAMP";
    private static final String CATALOG_ID = "CATALOG_ID";
    private static final String CATALOG_RESOURCE_URI = "CATALOG_RESOURCE_URI";
    private static final String CATALOG_DATATYPE = "CATALOG_DATATYPE";
    private static final String CATALOG_VERSION = "CATALOG_VERSION";
    
    private static final String FT_QUERY = "ftQuery";
    private static final String FULL_TEXT_QUERY_SQL = "TEXT_SEARCH_INDEX_COLUMN @@ "+FT_QUERY;
    
    private static final String END_PAREN = " ) ";
    private static final String START_PAREN = " ( ";
    private static final String OR = " OR ";
    private static final String AND = " AND ";
    
    public static final Map<String, String> FIELD_MAP;
    
    private PostgresNamespaceHandler namespaceHandler = null;
    private WKTReader reader = new WKTReader();
    
    private int ftQueryIndex = 1;
    
    static {
        Map<String, String> fieldMap = new HashMap<String, String>();
        // right now we don't search all text fields, so we give the most
        // popular
        fieldMap.put(Metacard.METADATA, CATALOG_METADATA );
        fieldMap.put(Metacard.ANY_TEXT, CATALOG_METADATA );
        fieldMap.put(Metacard.ANY_GEO, CATALOG_SOURCE_LOCATION );
        fieldMap.put(Metacard.GEOGRAPHY, CATALOG_SOURCE_LOCATION );
        fieldMap.put(Metacard.TITLE, CATALOG_TITLE );
        fieldMap.put(Metacard.EFFECTIVE, CATALOG_EFFECTIVE_DATE );
        fieldMap.put(Metacard.MODIFIED, CATALOG_MODIFIED_DATE );
        fieldMap.put(Metacard.EXPIRATION, CATALOG_EXPIRATION_DATE );
        fieldMap.put(Metacard.CREATED, CATALOG_CREATED_DATE );
        fieldMap.put( Metacard.ID, CATALOG_ID );
        fieldMap.put( Metacard.RESOURCE_URI, CATALOG_RESOURCE_URI );
        fieldMap.put( Metacard.CONTENT_TYPE, CATALOG_DATATYPE );
        fieldMap.put( Metacard.CONTENT_TYPE_VERSION, CATALOG_VERSION );
        
        FIELD_MAP = Collections.unmodifiableMap(fieldMap);
    }
    
    private void logEntry( String method, String name, Object value ){
        logger.debug( "ENTERING "+method+"( propertyName=["+name+"] , value=["+value+"] )" );
        verifyInputData(name, value);
    }
	
	public PostgresFilterDelegate( PostgresNamespaceHandler nsHandler ) {
		namespaceHandler = nsHandler;
	}

	@Override
	public SQLHolder and(List<SQLHolder> operands) {
		return logicalOperator(operands, AND);
	}

	@Override
	public SQLHolder or(List<SQLHolder> operands) {
		return logicalOperator(operands, OR);
	}

	@Override
	public SQLHolder not(SQLHolder operand) {
		return new SQLHolder(" NOT " + operand.getQuery(), operand);
	}

	@Override
    public SQLHolder propertyIsFuzzy(String propertyName, String searchPhrase) {
        logEntry( "propertyIsFuzzy", propertyName, searchPhrase );
        return handlePropertyIsLike( propertyName, searchPhrase, false );
        
    }
	
	@Override
	public SQLHolder propertyIsEqualTo(String propertyName, String literal, boolean isCaseSensitive) {
	    //TODO Add in logging of case sensitive flag
	    logEntry( isCaseSensitive ? "propertyIsEqualTo_CaseSensitive" : "propertyIsEqualTo", propertyName, literal );
	    
		// TODO Add in case sensitive support
		return new SQLHolder( getMappedPropertyName(propertyName) + " = '" + literal + "'" );
	}

	@Override
	public SQLHolder propertyIsEqualTo(String propertyName, Date exactDate) {
	    logEntry( "propertyIsEqualTo", propertyName, exactDate );

		//TODO figure out what string to use to pass in the java date field
		return new SQLHolder( getMappedPropertyName(propertyName) + " = " + exactDate );
	}

	@Override
	public SQLHolder propertyIsLike(String propertyName, String pattern, boolean isCaseSensitive) {
	    //TODO figure out how to log case senstive
	    logEntry( isCaseSensitive ? "propertyIsLike_CaseSensitive" : "propertyIsLike", propertyName, pattern );
	    return handlePropertyIsLike( propertyName, pattern, isCaseSensitive );
	}

	@Override
	public SQLHolder propertyIsGreaterThan(String propertyName, int literal) {
	    logEntry( "propertyIsGreaterThan", propertyName, literal );
		return getGreaterThanQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThan(String propertyName, short literal) {
	    logEntry( "propertyIsGreaterThan", propertyName, literal );
		return getGreaterThanQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThan(String propertyName, long literal) {
	    logEntry( "propertyIsGreaterThan", propertyName, literal );
		return getGreaterThanQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThan(String propertyName, float literal) {
	    logEntry( "propertyIsGreaterThan", propertyName, literal );
		return getGreaterThanQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThan(String propertyName, double literal) {
	    logEntry( "propertyIsGreaterThan", propertyName, literal );
		return getGreaterThanQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThanOrEqualTo(String propertyName, short literal) {
	    logEntry( "propertyIsGreaterThanOrEqualTo", propertyName, literal );
	    return getGreaterThanOrEqualToQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThanOrEqualTo(String propertyName, int literal) {
	    logEntry( "propertyIsGreaterThanOrEqualTo", propertyName, literal );
		return getGreaterThanOrEqualToQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThanOrEqualTo(String propertyName, long literal) {
	    logEntry( "propertyIsGreaterThanOrEqualTo", propertyName, literal );
	    return getGreaterThanOrEqualToQuery(propertyName, literal);
	}

	@Override
	public SQLHolder propertyIsGreaterThanOrEqualTo(String propertyName, float literal) {
	    logEntry( "propertyIsGreaterThanOrEqualTo", propertyName, literal );
	    return getGreaterThanOrEqualToQuery(propertyName, literal);
	}
	
	@Override
	public SQLHolder propertyIsGreaterThanOrEqualTo(String propertyName, double literal) {
	    logEntry( "propertyIsGreaterThanOrEqualTo", propertyName, literal );
	    return getGreaterThanOrEqualToQuery(propertyName, literal);
	}
	
	@Override
	public SQLHolder during(String propertyName, Date startDate, Date endDate) {
	    //TODO figure out how to format dates
	    //TODO add in logging
		return new SQLHolder( getMappedPropertyName(propertyName) + " between '" +  startDate + "' AND '" +  endDate + "'" );
	}

	@Override
	public SQLHolder before(String propertyName, Date date) {
	  //TODO figure out how to format dates
	    logEntry( "before", propertyName, date );
	    
	    return new SQLHolder( getMappedPropertyName(propertyName) + " between 'epoch' AND '" +  date + "'" );
	}
	
	@Override
	public SQLHolder after(String propertyName, Date date) {
	    //TODO figure out how to format dates
	    logEntry( "after", propertyName, date );
        
        return new SQLHolder( getMappedPropertyName(propertyName) + " between '" + date + "' AND 'infinity'" );
    }

	@Override
	public SQLHolder relative(String propertyName, long duration) {
	  //TODO figure out how to format dates
	    logEntry( "relative", propertyName, duration );
		Date date = new DateTime().minus(duration).toDate();
		return new SQLHolder( getMappedPropertyName( propertyName) + " between '" + date + "' AND '" + new Date() + "'" );
	}

    @Override
    public SQLHolder nearestNeighbor(String propertyName, String wkt) {
        logEntry( "nearestNeighbor", propertyName, wkt );
        return handleDistanceWithin( propertyName, wkt, NEAREST_NEIGHBOR_RADIUS );
    }
    
	@Override
	public SQLHolder contains(String propertyName, String wkt) {
	    logEntry( "contains", propertyName, wkt );
	    return geoOperationToQuery("ST_CONTAINS", propertyName, wkt);
	}

	@Override
	public SQLHolder dwithin(String propertyName, String wkt, double distance) {
	    //TODO log more than 2 parameters
	    logEntry( "dwithin", propertyName, wkt );
	    return handleDistanceWithin( propertyName, wkt, distance );
	}

	@Override
	public SQLHolder intersects(String propertyName, String wkt) {
	    logEntry( "intersects", propertyName, wkt );
        return geoOperationToQuery("ST_INTERSECTS", propertyName, wkt);
	}

	@Override
	public SQLHolder within(String propertyName, String wkt) {
	    logEntry( "within", propertyName, wkt );
	    //TODO figure out what within is
        return geoOperationToQuery("ST_INTERSECTS", propertyName, wkt);
	}

	@Override
	public SQLHolder disjoint(String propertyName, String wkt) {
	    logEntry( "disjoint", propertyName, wkt );
        //TODO figure out what within is
        return geoOperationToQuery("ST_INTERSECTS", propertyName, wkt);
    }

	@Override
	public SQLHolder overlaps(String propertyName, String wkt) {
	    logEntry( "overlaps", propertyName, wkt );
        //TODO figure out what within is
        return geoOperationToQuery("ST_OVERLAPS", propertyName, wkt);
    }
	
	@Override
	public SQLHolder xpathExists( String xpath ){
	    logEntry( "xpathExists", Metacard.METADATA, xpath );
	    xpath = normalizeDefaultNamespaces( normalizeDefaultNamespaces( normalizeDefaultNamespaces( xpath, new StringBuilder(), '[' ), new StringBuilder(), '/' ), new StringBuilder(), '@' );
	    return new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " );
	    //return new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" );
	}
	 
	@Override 
	public SQLHolder xpathIsLike(String xpath, String pattern, boolean isCaseSensitive){
	    logEntry( isCaseSensitive ? "xpathIsLike_CaseSensitive" : "xpathIsLike", pattern, xpath );
	    xpath = normalizeDefaultNamespaces( normalizeDefaultNamespaces( normalizeDefaultNamespaces( xpath, new StringBuilder(), '[' ), new StringBuilder(), '/' ), new StringBuilder(), '@' );
	    int index = xpath.indexOf( "/@" );
        SQLHolder returnHolder = null;
	    if ( index >= 0 ){
            xpath = xpath.replace( "/@", "[@" );
            xpath += "=\""+pattern+"\"]";
            returnHolder = new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " );
            //returnHolder = new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" );
        }else{
            List<SQLHolder> holders = new ArrayList<SQLHolder>();
            holders.add( propertyIsLike( Metacard.ANY_TEXT, pattern, isCaseSensitive ) );
            holders.add( new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " ) );
            //holders.add( new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" ) );
            returnHolder = this.and( holders );
        }
	    return returnHolder;
	}

    @Override 
	public SQLHolder xpathIsFuzzy(String xpath, String literal){
        logEntry( "xpathIsFuzzy", Metacard.METADATA , xpath );
        xpath = normalizeDefaultNamespaces( normalizeDefaultNamespaces( normalizeDefaultNamespaces( xpath, new StringBuilder(), '[' ), new StringBuilder(), '/' ), new StringBuilder(), '@' );
        int index = xpath.indexOf( "/@" );
        SQLHolder returnHolder = null;
        if ( index >= 0 ){
            xpath.replace( "/@", "[@" );
            xpath += "=\""+literal+"\"]";
            returnHolder = new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " );
            //returnHolder = new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" );
        }else if ( literal != null && !literal.isEmpty() ){
            List<SQLHolder> holders = new ArrayList<SQLHolder>();
            holders.add( propertyIsFuzzy( Metacard.ANY_TEXT, literal ) );
            holders.add( new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " ) );
            //holders.add( new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" ) );
            returnHolder = this.and( holders );
        }else{
        	returnHolder = new SQLHolder( "array_length(xpath( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" ),1) is not null " );
        	//returnHolder = new SQLHolder( "xpath_exists( '"+xpath+"', xml(catalog_metadata), "+namespaceHandler.getNamespacesAsString()+" )" );
        }
        return returnHolder;
	}

	private SQLHolder getGreaterThanQuery(String propertyName, Number literal) {
	    return new SQLHolder( getMappedPropertyName(propertyName) + " >= " + literal.toString() );
	}
	
	private SQLHolder getGreaterThanOrEqualToQuery(String propertyName, Number literal) {
        return new SQLHolder( getMappedPropertyName(propertyName) + " >= " + literal.toString() );
    }

	private String getMappedPropertyName(String propertyName) {
	    String name = FIELD_MAP.get(propertyName);
		if ( name == null ){
		    throw new UnsupportedOperationException( "The propertyName '" + propertyName +"' is not supported to query against" );
		}
	    return name;
	}

	private SQLHolder logicalOperator(List<SQLHolder> operands, String operator) {

		if (operands == null || operands.size() < 1) {
			throw new UnsupportedOperationException("[" + operator + "] operation must contain 1 or more filters.");
		}

		int startIndex = 0;

		SQLHolder query = operands.get(startIndex);

		startIndex++;

		if (query == null) {
			throw new UnsupportedOperationException("Query was not interpreted properly. Query should not be null.");
		}

		StringBuilder builder = new StringBuilder();

		builder.append(START_PAREN);

		builder.append(query.getQuery());

		for (int i = startIndex; i < operands.size(); i++) {

			SQLHolder localQuery = operands.get(i);

			if (localQuery != null) {
				String localPhrase = localQuery.getQuery();
				builder.append(operator + localPhrase);
				String localTsQuery = localQuery.getFromStmts();
				if ( localTsQuery != null ){
				    query.addFromStmt( localTsQuery );
				}
				if ( !query.isAddRelevance() ){
				    query.setAddRelevance( localQuery.isAddRelevance() );
				}
				if ( !query.isAddDistance() ){
                    query.setAddDistance( localQuery.isAddDistance() );
                }
				if ( query.getWkt() == null ){
				    query.setWkt( localQuery.getWkt() );
				}
			} else {
				throw new UnsupportedOperationException("Query was not interpreted properly. Query should not be null.");
			}

		}

		builder.append(END_PAREN);

		query.setQuery(builder.toString());

		return query;
	}

	private void verifyInputData(String propertyName, Object pattern) {
		if (propertyName == null || propertyName.isEmpty()) {
			throw new UnsupportedOperationException("PropertyName is required for search.");
		}
		if (pattern == null || ( pattern instanceof String && ((String)pattern).isEmpty() ) ) {
			throw new UnsupportedOperationException("Literal value is required for search.");
		}
	}

	private SQLHolder geoOperationToQuery(String operation, String propertyName, String wkt) {
        String columnName = getMappedPropertyName(propertyName);
        
        wkt = wkt.toUpperCase().trim();
        if ( wkt.startsWith( GEOMETRYCOLLECTION ) ){
        	try {
				Geometry geo = reader.read( wkt );
				List<SQLHolder> sqlHolders = new ArrayList<SQLHolder>();
				int numGeos = geo.getNumGeometries();
				for( int i=0; i<numGeos; i++ ){
					wkt = geo.getGeometryN( i ).toText();
					sqlHolders.add( new SQLHolder( columnName +
							"&& ST_GeomFromEWKT('SRID=4326;" + wkt + "') AND " + 
							operation + 
							"( ST_GeomFromEWKT('SRID=4326;" + wkt + "'), " + 
							columnName + ")" ) 
					);
				}
				return this.or( sqlHolders );
			} catch (ParseException e) {
				String message = "Invalid WKT '" + wkt + "': " + e.getMessage();
				logger.error( message );
				throw new UnsupportedOperationException( message );
			}
        }else{
        	return new SQLHolder( columnName +
                    "&& ST_GeomFromEWKT('SRID=4326;" + wkt + "') AND " + 
                    operation + 
                    "( ST_GeomFromEWKT('SRID=4326;" + wkt + "'), " + 
                    columnName + ")" ) ;
        } 
	}
	
	private SQLHolder handleDistanceWithin( String propertyName, String wkt, double distance ){
        String name = getMappedPropertyName(propertyName);
	    SQLHolder holder = new  SQLHolder( " ST_DWithin( " + name + ",  ST_GeomFromEWKT('SRID=4326;" + wkt + "'), " + metersToDegrees( distance ) + " )" + 
	            " AND " + 
	            "ST_Distance_Sphere(" + name + ",  ST_GeomFromEWKT('SRID=4326;" + wkt + "')) < '"+distance+"'"
            );
	    holder.setAddDistance( true );
	    holder.setWkt( wkt );
	    return holder;
    }
    
    private SQLHolder handlePropertyIsLike( String propertyName, String searchPhrase, boolean isCaseSensitive ){
        
        String mappedPropertyName = getMappedPropertyName(propertyName);
        
        SQLHolder query = null;
        if ( Metacard.ANY_TEXT.equals( propertyName) ){
            searchPhrase = searchPhrase.trim();
            String[] words = StringUtils.split( searchPhrase );
            
            int wildcardIndex = words[0].indexOf( '*' );
            // Need to add in extra logic here to handle if it is not querying the metadata exactly
            if ( words.length == 1 && isValidTSQuery( wildcardIndex, searchPhrase ) && !containsSpecialCharacter(searchPhrase)){
                
                query = new SQLHolder( FULL_TEXT_QUERY_SQL + ftQueryIndex );
                query.addFromStmt( ", to_tsquery( '"+ handleWildcards( words[0] )+"' ) "+ FT_QUERY + ftQueryIndex++ );
                query.setAddRelevance( true );
                if ( isCaseSensitive ){
                    query = and( Arrays.asList( query, new SQLHolder( mappedPropertyName + " like '%" + searchPhrase.replace( "*", "" ) + "%'" ) ) );
                }
            }
            if ( query == null ){
                if ( wildcardIndex == 0  ){
                    searchPhrase = searchPhrase.substring( 1 );
                }
                if ( searchPhrase.endsWith( "*" ) ){
                    searchPhrase = searchPhrase.substring( 0, searchPhrase.length() -1 );
                }
                query = new SQLHolder( mappedPropertyName + 
                        ( isCaseSensitive ? " like '%" : " ilike '%" ) 
                        + searchPhrase.replace( "*", "%" ) + "%'" );
            }

        }else{
            if ( searchPhrase.indexOf( '*' ) >= 0 ){
                query = new SQLHolder( mappedPropertyName + 
                        ( isCaseSensitive ? " like " : " ilike '" ) 
                        + searchPhrase.replace( "*", "%" ) + "'");
            }else{
                query = new SQLHolder( mappedPropertyName + " = '" + searchPhrase + "'" );
            }
        }
        return query;
    }
    
    private boolean isValidTSQuery( int wildcardIndex, String searchPhrase ) {
        if ( wildcardIndex == 0 ){
                return false;
        }if ( wildcardIndex == 1 ){
            char start = searchPhrase.toLowerCase().charAt( 0 );
            return !( start == 'a' || start =='i'  );
        }
        return true;
    }

    private String handleWildcards( String string ) {
        int index = string.indexOf( '*' );
        if ( index > 0 ){
            string = string.substring( 0, index ) + ":*";
        }
        return string;
    }

    private double metersToDegrees(double distance) {
        return distance/DEGREES_CONVERSTION_DEFAULT;
    }
    
    protected static String normalizeDefaultNamespaces( String textPathSection, StringBuilder sb, char delimitter ) {
        int delimitterIndex = textPathSection.indexOf( delimitter );
        if ( delimitterIndex > -1 ){
            sb.append( textPathSection.substring(0, delimitterIndex + 1 ) );
            String tempString = textPathSection.substring( delimitterIndex + 1 );
            if ( tempString.indexOf( delimitter ) == 0 ){
                tempString = tempString.substring( 1 );
                sb.append( delimitter );
            }
            delimitterIndex = tempString.indexOf( '/' );
            int colonIndex = tempString.indexOf( ':' );
            if ( delimitterIndex == colonIndex ){
                sb.append( DEFAULT_NAMESPACE );
                sb.append( tempString );
            }else if ( delimitterIndex == -1 ){
                sb.append( tempString );
            }else if ( delimitterIndex < colonIndex || colonIndex == -1){
                sb.append( DEFAULT_NAMESPACE );
                sb.append( tempString.substring( 0, delimitterIndex ) );
                normalizeDefaultNamespaces( tempString.substring( delimitterIndex ), sb, '/' );
            }else{
                sb.append( tempString.substring( 0, colonIndex ) );
                normalizeDefaultNamespaces( tempString.substring( colonIndex ), sb, '/' );
            }
        }else{
            return textPathSection;
        }
        return sb.toString();
    }
    
    private boolean containsSpecialCharacter( String string ) {
        return string.indexOf( '!') > -1;
    }
	
	
}

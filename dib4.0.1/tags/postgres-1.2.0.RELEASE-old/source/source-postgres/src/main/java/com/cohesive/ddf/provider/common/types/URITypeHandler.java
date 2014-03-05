/**
 *  Copyright Cohesive Integrations - 2011
 */
package com.cohesive.ddf.provider.common.types;

import java.net.URI;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Nguessan Kouame
 * @author Jeff Vettraino
 * TypeHandler for XML Type
 */
public class URITypeHandler extends BaseTypeHandler<URI> {

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#getNullableResult(java.sql.ResultSet, java.lang.String)
	 */
	@Override
	public URI getNullableResult(ResultSet resultSet, String columnName)	throws SQLException {
		
		return URI.create( resultSet.getString(columnName) );
	}

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#getNullableResult(java.sql.CallableStatement, int)
	 */
	@Override
	public URI getNullableResult(CallableStatement callableStatement, int columnIndex) throws SQLException {
	    return URI.create( callableStatement.getString(columnIndex) );
	}

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#setNonNullParameter(java.sql.PreparedStatement, int, java.lang.Object, org.apache.ibatis.type.JdbcType)
	 */
	@Override
	public void setNonNullParameter(PreparedStatement preparedStatement, int i, URI parameter, JdbcType jdbcType) throws SQLException {
		preparedStatement.setString(i, ( (URI)parameter).toASCIIString() );
	}

	@Override
	public URI getNullableResult(ResultSet resultSet, int i)
			throws SQLException {
		return URI.create( resultSet.getString(i) );
	}

}

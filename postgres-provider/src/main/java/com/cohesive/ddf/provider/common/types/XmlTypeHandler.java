/**
 *  Copyright Cohesive Integrations - 2011
 */
package com.cohesive.ddf.provider.common.types;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Nguessan Kouame
 * @author Jeff Vettraino
 * TypeHandler for XML Type
 */
public class XmlTypeHandler extends BaseTypeHandler<String> {

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#getNullableResult(java.sql.ResultSet, java.lang.String)
	 */
	@Override
	public String getNullableResult(ResultSet resultSet, String columnName)	throws SQLException {
		
		SQLXML sqlxml = resultSet.getSQLXML(columnName);
		return sqlxml.getString();
	}

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#getNullableResult(java.sql.CallableStatement, int)
	 */
	@Override
	public String getNullableResult(CallableStatement callableStatement, int columnIndex) throws SQLException {

		SQLXML sqlxml = callableStatement.getSQLXML(columnIndex);
		return sqlxml.getString();
	}

	/**
	 * @see org.apache.ibatis.type.BaseTypeHandler#setNonNullParameter(java.sql.PreparedStatement, int, java.lang.Object, org.apache.ibatis.type.JdbcType)
	 */
	@Override
	public void setNonNullParameter(PreparedStatement preparedStatement, int i, String parameter, JdbcType jdbcType) throws SQLException {
		SQLXML sqlxml = preparedStatement.getConnection().createSQLXML();
		sqlxml.setString( parameter );
		preparedStatement.setSQLXML(i, sqlxml);
	}

	@Override
	public String getNullableResult(ResultSet resultSet, int columnIndex)
			throws SQLException {
		SQLXML sqlxml = resultSet.getSQLXML( columnIndex );
		return sqlxml.getString();
	}

}

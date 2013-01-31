/**
 *  Copyright Cohesive Integrations - 2011
 */
package com.cohesive.ddf.provider.persistence;

import java.util.List;

import ddf.catalog.data.ContentType;

/**
 * @author Cohesive Integrations LLC
 */
public interface ContentTypeMapper{
	
	public List<ContentType> getContentTypes();
}
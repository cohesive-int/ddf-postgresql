/**
 *  Copyright Cohesive Integrations - 2011
 */
package com.cohesive.ddf.provider.persistence;

import java.net.URI;
import java.util.List;

import com.cohesive.ddf.provider.common.ProviderMetacard;
import com.cohesive.ddf.provider.common.UpdateByURIRequest;



/**
 * @author Nguessan Kouame
 * @author Jeff Vettraino
 */
public interface IngestMapper {

	public void createMetacard( ProviderMetacard metacard );
	//public void batchCreateMetacard( CreateMetacardRequest createRequest );
	
	public int updateMetacard( ProviderMetacard record );
	public int updateMetacardByURI( UpdateByURIRequest request );
	
	public int deleteMetacards( List<String> ids );
	public int deleteMetacardsByURI( List<URI> uris );

	
}

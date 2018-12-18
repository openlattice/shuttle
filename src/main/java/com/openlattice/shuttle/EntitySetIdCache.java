package com.openlattice.shuttle;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class EntitySetIdCache {
    private Logger logger = LoggerFactory.getLogger( EntitySetIdCache.class );
    private static transient LoadingCache<String, UUID> entitySetIdCache = null;

    public UUID getEntitySetId(
            EntityDefinition entityDefinition,
            boolean createEntitySets,
            EdmApi edmApi,
            Set<String> contacts ) {
        try {
            return entitySetIdCache.get( entityDefinition.getEntitySetName() );
        } catch ( Exception e ) {
            logger.warn( "Unable to retrieve entity set {}", entityDefinition.getEntitySetName() );
            if ( createEntitySets ) {
                logger.info( "Creating entity set {}", entityDefinition.getEntitySetName() );
                UUID entitySetId = edmApi.createEntitySets(
                        Set.of(
                                new EntitySet(
                                        entityDefinition.getId().orElse( UUID.randomUUID() ),
                                        edmApi.getEntityTypeId( entityDefinition.getEntityTypeFqn() ),
                                        entityDefinition.getEntitySetName(),
                                        entityDefinition.getEntitySetName(),
                                        Optional.of( entityDefinition.getEntitySetName() ),
                                        contacts ) ) )
                        .get( entityDefinition.getEntitySetName() );
                entitySetIdCache.put( entityDefinition.getEntitySetName(), entitySetId );
                return entitySetId;
            }
            return null;
        }
    }

    public UUID getEntitySetIdUnchecked( EntityDefinition entityDefinition ) {
        return entitySetIdCache.getUnchecked( entityDefinition.getEntitySetName() );
    }

    public void initializeEntitySetIdCache( EdmApi edmApi ) {
        if ( entitySetIdCache == null ) {
            entitySetIdCache = CacheBuilder
                    .newBuilder()
                    .maximumSize( 1000 )
                    .build( new CacheLoader<String, UUID>() {
                        @Override
                        public UUID load( String entitySetName ) throws Exception {
                            return edmApi.getEntitySetId( entitySetName );
                        }
                    } );
        }
    }
}

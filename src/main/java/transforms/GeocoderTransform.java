package transforms;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.retrofit.RhizomeByteConverterFactory;
import com.openlattice.retrofit.RhizomeCallAdapterFactory;
import com.openlattice.retrofit.RhizomeJacksonConverterFactory;
import com.openlattice.rhizome.proxy.RetrofitBuilders;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.http.Query;

public class GeocoderTransform extends Transformation<Object> {
    protected static final String       NOMINATIM_SERVICE_URL = "https://osm.openlattice.com/nominatim/";
    private final          String       addressObject;
    private final          GeocodingApi geocodingApi;

    /**
     * Represents a transformation to get the digits at the start of a column (if starts with digits).
     *
     * @param addressObject: which object from the address to choose, on of ['lat', 'lon', 'type', 'house_number'
     * 'road', 'neighbourhood', 'city', 'postcode', 'county', 'state',
     * 'country', 'country_code']
     * @param column: column name
     */
    @JsonCreator
    public GeocoderTransform(
            @JsonProperty( Constants.ADDRESS_OBJECT ) String addressObject,
            @JsonProperty( Constants.COLUMN ) Optional<String> column ) {
        super( column );
        this.addressObject = addressObject.equals( "street" ) ? "road" : addressObject;
        this.geocodingApi = new Retrofit.Builder()
                .baseUrl( NOMINATIM_SERVICE_URL )
                .addConverterFactory( new RhizomeByteConverterFactory() )
                .addConverterFactory( new RhizomeJacksonConverterFactory( ObjectMappers.getJsonMapper() ) )
                .addCallAdapterFactory( new RhizomeCallAdapterFactory() )
                .client( RetrofitBuilders.okHttpClient().build() )
                .build().create( GeocodingApi.class );
    }

    public String getAddress( String input, String addressObject ) {
        List<Map<String, Object>> map = geocodingApi.geocode( addressObject );

        if ( map.size() == 0 ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }

        Map<String, Object> address = map.get( 0 );
        if ( address.isEmpty() ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }

        List<String> outercodes = Arrays.asList( "lat", "lon", "type" );
        if ( outercodes.contains( addressObject ) ) {
            return (String) address.get( addressObject );
        } else {
            Map<String, Object> hlpr = (Map<String, Object>) address.get( "address" );
            return (String) hlpr.get( addressObject );
        }

    }

    @Override
    public String applyValue( String input ) {
        if ( input == null ) { return null; }

        return getAddress( input, this.addressObject );
    }

    public interface GeocodingApi {
        @GET( "search?"
                + "format=json"
                + "&addressdetails=1"
                + "&limit=" + 5 )
        List<Map<String, Object>> geocode( @Query( "q" ) String address );
    }

}


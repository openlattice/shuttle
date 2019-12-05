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
import java.util.*;
import com.openlattice.shuttle.util.Parsers;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.http.Query;

public class GeocoderTransform extends Transformation<Object> {
    protected static final String       NOMINATIM_SERVICE_URL = "https://osm.openlattice.com/nominatim/";

    // catalog of constants expected as input by this transformation OR by the geocodingAPI
    private static final   Map<String, String> KEYWORD_MAP;
    static {
        HashMap<String, String> dummyMap = new HashMap<>();
        dummyMap.put( "lat", "lat" );
        dummyMap.put( "lon", "lon" );
        dummyMap.put( "geographypoint", "geographypoint" );
        dummyMap.put( "type", "type" );
        dummyMap.put( "house_number", "house_number" );
        dummyMap.put( "road", "road" );
        dummyMap.put( "neighborhood", "neighborhood" );
        dummyMap.put( "city", "city" );
        dummyMap.put( "postcode", "postcode" );
        dummyMap.put( "county", "county" );
        dummyMap.put( "state", "state" );
        dummyMap.put( "address", "address" );
        KEYWORD_MAP = Collections.unmodifiableMap(dummyMap);
    }

    // values of addressObject that can be looked up directly in geocoder api output
    private static final   HashSet<String> SIMPLE_LOOKUP;
    static {
        HashSet<String> dummySet = new HashSet<>();
        dummySet.add( "lat" );
        dummySet.add( "lon" );
        dummySet.add( "type" );
        SIMPLE_LOOKUP = dummySet;
    }

    private final          String       addressObject;
    private final          GeocodingApi geocodingApi;

    /**
     * A transformation that runs a string address through a geolocation API and returns a user-specified part of the
     * resulting data.
     *
     *
     * @param addressObject: which object from the address to choose, one of ['lat', 'lon', 'geographypoint', 'type', 'house_number'
     * 'road', 'neighbourhood', 'city', 'postcode', 'county', 'state',
     * 'country', 'country_code']
     * @param column: column name
     *
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
        if ( !KEYWORD_MAP.containsKey( addressObject ) || addressObject.equals( "address" ) ) {
            throw new NoSuchElementException("addressObject invalid: " + this.addressObject);
        }
        List<Map<String, Object>> map = geocodingApi.geocode( input );

        if ( map.size() == 0 ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }

        Map<String, Object> address = map.get( 0 );

        if ( address.isEmpty() ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }

        if ( SIMPLE_LOOKUP.contains( addressObject ) ) {
            return (String) address.get( addressObject );
        }

        if ( addressObject.equals( KEYWORD_MAP.get( "geographypoint" ) ) ) {
            Double lat = Parsers.parseDouble(address.get( KEYWORD_MAP.get( "lat" ) ));
            Double lon = Parsers.parseDouble(address.get( KEYWORD_MAP.get( "lon" ) ));
            if ( lat == null || lon == null ) {
                return null;
            }
            return String.format("%.5f", lat) + "," + String.format("%.5f", lon);
        }

        Map<String, Object> hlpr = (Map<String, Object>) address.get( KEYWORD_MAP.get( "address" ) );
        return (String) hlpr.get( addressObject );

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


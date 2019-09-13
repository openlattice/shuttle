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
import java.util.NoSuchElementException;

import com.openlattice.shuttle.util.Parsers;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.http.Query;

public class GeocoderTransform extends Transformation<Object> {
    protected static final String       NOMINATIM_SERVICE_URL = "https://osm.openlattice.com/nominatim/";
    private final          String       addressObject;
    private final          GeocodingApi geocodingApi;

    // string constants for flight + geocoder api options
    private final          String       LAT = "lat";
    private final          String       LON = "lon";
    private final          String       GEOGRAPHY_POINT = "geographypoint";
    private final          String       TYPE = "type";
    private final          String       HOUSE_NUMBER = "house_number";
    private final          String       ROAD = "road";
    private final          String       NEIGHBORHOOD = "neighborhood";
    private final          String       CITY = "city";
    private final          String       POSTCODE = "postcode";
    private final          String       COUNTY = "county";
    private final          String       STATE = "state";
    private final          String       ADDRESS = "address";

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
        if ( !Arrays.asList(LAT, LON, GEOGRAPHY_POINT, TYPE, HOUSE_NUMBER,
                ROAD, NEIGHBORHOOD, CITY, POSTCODE, COUNTY, STATE).contains(this.addressObject) ) {
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

        if ( Arrays.asList( LAT, LON, TYPE ).contains( addressObject ) ) {
            return (String) address.get( addressObject );
        }

        if ( addressObject.equals(GEOGRAPHY_POINT) ) {
            Double lat = Parsers.parseDouble(address.get(LAT));
            Double lon = Parsers.parseDouble(address.get(LON));
            if ( lat == null || lon == null ) {
                return null;
            }
            return String.format("%.5f", lat) + "," + String.format("%.5f", lon);
        }

        Map<String, Object> hlpr = (Map<String, Object>) address.get( ADDRESS );
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


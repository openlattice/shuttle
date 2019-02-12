package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GeocoderTransform extends Transformation<Object> {
    private final String addressObject;
    protected     String NOMINATIM_SERVICE_URL = "https://osm.openlattice.com/nominatim/";

    /**
     * Represents a transformation to get the digits at the start of a column (if starts with digits).
     *
     * @param addressObject: which object from the address to choose, on of ['lat', 'lon', 'type', 'house_number'
     *                       'road', 'neighbourhood', 'city', 'postcode', 'county', 'state',
     *                       'country', 'country_code']
     * @param column:        column name
     */
    @JsonCreator
    public GeocoderTransform(
            @JsonProperty( Constants.ADDRESS_OBJECT ) String addressObject,
            @JsonProperty( Constants.COLUMN ) Optional<String> column ) {
        super( column );
        this.addressObject = addressObject == "street" ? "road" : addressObject;
    }

    public String getAddress( String input, String addressObject ) throws java.io.IOException {

        // GEOCODE

        URL url = new URL( NOMINATIM_SERVICE_URL
                + "search?"
                + "format=json"
                + "&addressdetails=1"
                + "&limit=" + 5
                + "&q=" + URLEncoder.encode( input ) );

        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod( "GET" );

        int status = con.getResponseCode();
        if ( status != 200 ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }

        // READ IN RESULTS

        BufferedReader in = new BufferedReader(
                new InputStreamReader( con.getInputStream() ) );
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ( ( inputLine = in.readLine() ) != null ) {
            content.append( inputLine );
        }

        in.close();
        con.disconnect();

        // READ TO MAP AND GET FIRST

        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> map = mapper
                .readValue( content.toString(), new TypeReference<List<Map<String, Object>>>() {
                } );
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
            Object hlp1 = address.get( "address" );
            Map<String, Object> hlpr = mapper.convertValue( hlp1, Map.class );
            return (String) hlpr.get( addressObject );
        }

    }

    @Override
    public String applyValue( String input ) {
        try {
            return getAddress( input, this.addressObject );
        } catch ( java.io.IOException e ) {
            System.out.println( "Can't parse address " + input );
            return null;
        }
    }

}


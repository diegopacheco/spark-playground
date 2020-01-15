import com.fasterxml.jackson.databind.ObjectMapper;

public class Converter {
    private static ObjectMapper mapper = new ObjectMapper();
    
    public static WordEvent convert(String json) {
		try {
			return mapper.readValue(json, WordEvent.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to convert json to object", e);
		}
	}
}
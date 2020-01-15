public class Converter {
    public static WordEvent convert(String content) {
		try {
			if (content==null){
				content="";
			}
			return new WordEvent(content);
		} catch (Exception e) {
			throw new RuntimeException("Failed to convert json to object", e);
		}
	}
}
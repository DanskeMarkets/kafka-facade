package dk.danskebank.markets.kafka.serialization;

import lombok.Data;

import java.util.List;

@Data
class TestData {
	private List<String> stringValues;
	private List<Integer> intValues;
	private List<Double> doubleValues;
	private List<NestedData> nestedValues;

	@Data
	static class NestedData {
		private String field1;
		private Boolean field2;
	}
}

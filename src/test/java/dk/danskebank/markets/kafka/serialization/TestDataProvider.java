package dk.danskebank.markets.kafka.serialization;

import lombok.val;

import java.util.ArrayList;

final class TestDataProvider {

	static TestData getData() {
		val stringValues = new ArrayList<String>();
		stringValues.add("Test1");
		stringValues.add("Test 5457");
		stringValues.add("");
		stringValues.add(null);

		val intValues = new ArrayList<Integer>();
		intValues.add(0);
		intValues.add(150000000);
		intValues.add(-1);
		intValues.add(null);

		val doubleValues = new ArrayList<Double>();
		doubleValues.add(0.0);
		doubleValues.add(1e-100);
		doubleValues.add(-56.3556446);
		doubleValues.add(null);

		val nestedValues = new ArrayList<TestData.NestedData>();
		nestedValues.add(new TestData.NestedData()
				.setField1("Epic")
				.setField2(false));
		nestedValues.add(new TestData.NestedData()
				.setField1("Sample"));
		nestedValues.add(new TestData.NestedData()
				.setField2(true));
		nestedValues.add(new TestData.NestedData());
		nestedValues.add(null);

		return new TestData()
				.setStringValues(stringValues)
				.setIntValues(intValues)
				.setDoubleValues(doubleValues)
				.setNestedValues(nestedValues);
	}

	private TestDataProvider() { }
}

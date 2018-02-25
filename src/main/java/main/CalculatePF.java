package  main;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class CalculatePF extends UserDefinedAggregateFunction {

	private StructType inputSchema;
	private StructType bufferSchema;

	public CalculatePF() {
		List<StructField> inputFields = new ArrayList();
		inputFields.add(DataTypes.createStructField("W", DataTypes.DoubleType, true));
		inputFields.add(DataTypes.createStructField("VA", DataTypes.DoubleType, true));
		inputSchema = DataTypes.createStructType(inputFields);

		List<StructField> bufferFields = new ArrayList();
		bufferFields.add(DataTypes.createStructField("PF", DataTypes.DoubleType, true));
		bufferSchema = DataTypes.createStructType(bufferFields);
	}
	// Data types of input arguments of this aggregate function
	public StructType inputSchema() {
		return inputSchema;
	}
	// Data types of values in the aggregation buffer
	public StructType bufferSchema() {
		return bufferSchema;
	}
	// The data type of the returned value
	public DataType dataType() {
		return DataTypes.DoubleType;
	}
	// Whether this function always returns the same output on the identical input
	public boolean deterministic() {
		return true;
	}
	// Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
	// standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
	// the opportunity to update its values. Note that arrays and maps inside the buffer are still
	// immutable.
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0,0.0);
	}
	// Updates the given aggregation buffer `buffer` with new input data from `input`
	public void update(MutableAggregationBuffer buffer, Row input) {
		if (!input.isNullAt(0) && !input.isNullAt(1)) {
			double average = input.getDouble(0)/input.getDouble(1);
			buffer.update(0, average);
		}
	}
	// Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
		long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
		buffer1.update(0, mergedSum);
		buffer1.update(1, mergedCount);
	}
	// Calculates the final result
	public Double evaluate(Row buffer) {
		return ((double) buffer.getLong(0)) / buffer.getLong(1);
	}
}
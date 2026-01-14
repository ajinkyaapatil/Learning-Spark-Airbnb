# Test Plan: city_wise_data

## Objective
To ensure the `city_wise_data` transformation logic is robust, handles edge cases, and calculates metrics correctly across different data scenarios.

## Missing Test Scenarios

### 1. Multiple Cities Grouping
- **Description**: Verify that metrics are aggregated independently for each city.
- **Test Case**: Input containing rows for at least two different cities (e.g., "Pune", "Mumbai").
- **Expected Result**: A DataFrame with one row per city, with calculations specific to that city's subset of data.

### 2. Boundary Value Analysis (365)
- **Description**: Verify the edge case of the filter condition (`<= 365`).
- **Test Case**: A row where `availability_365` is exactly "365".
- **Expected Result**: The row should be **included** in the aggregation.

### 3. Null and Missing Data Handling
- **Description**: Verify stability when encountering null values.
- **Test Case**: 
    - `availability_365` is `None`.
    - `city` is `None`.
- **Expected Result**: `None` availability should be filtered out (as comparison with null fails). `None` city should group into a `null` city row.

### 4. Empty Input Handling
- **Description**: Ensure the function does not crash on empty data.
- **Test Case**: An empty DataFrame matching `input_schema`.
- **Expected Result**: An empty DataFrame matching `extracted_schema`.

### 5. Data Type Resilience (Invalid Strings)
- **Description**: Verify behavior when `StringType` column `availability_365` contains non-numeric text.
- **Test Case**: Values like "N/A", "abc", or empty strings "".
- **Expected Result**: These values should be filtered out or result in nulls that do not break the aggregation logic.

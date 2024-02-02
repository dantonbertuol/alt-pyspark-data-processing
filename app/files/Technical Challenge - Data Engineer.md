### Technical Challenge: Data Pipeline Implementation

**Problem Statement:**

You are given a dataset stored in a CSV file named "user_logs.csv." The dataset contains user interaction logs from various sources and includes additional columns: `user_id`, `timestamp`, `action`, `source`, and `device_type`.

Your goal is to build an advanced data pipeline using PySpark/Python to perform the following tasks:

1. **Convert CSV to Parquet:**
   - Read the "user_logs.csv" file into a PySpark DataFrame.
   - Transform and clean the data as needed.
   - Save the transformed data as a Parquet file named "user_logs.parquet."

2. **Advanced Data Analysis:**
   - Read the "user_logs.parquet" file into a PySpark DataFrame.
   - Implement data cleansing to handle missing values and outliers in the dataset.
   - Perform a time-based aggregation to calculate the count of each unique action for each user, broken down by source and device type.
   - Identify the top 3 actions (based on count) for each user and each unique combination of source and device type.
   - Write the final results to a new Parquet file named "output_results.parquet" with columns: `user_id`, `source`, `device_type`, `top_action`, `action_count`.

**Instructions:**

- Use PySpark or Python for the implementation.
- Comment your code appropriately for clarity.
- Assume that PySpark and required dependencies are already installed.
- Provide a brief explanation of your approach and any assumptions made.

**Evaluation Criteria:**

You will be assessed based on the following criteria:

1. **Code correctness:** Ensure that the code accurately processes and analyzes the data.
2. **Data cleansing and handling:** Implement robust data cleansing techniques to handle missing values and outliers.
3. **CSV to Parquet transformation:** Successfully transform the CSV file to Parquet.
4. **Advanced data processing:** Demonstrate an understanding of more complex data transformations and aggregations.
5. **Efficiency:** Consider efficiency in your code, especially for aggregations and sorting operations.
6. **Handling of edge cases:** Account for potential challenges in real-world data.

**Submission:**

You can submit your solution in the form of a Python script or a Jupyter notebook, along with any necessary instructions to run or explanations. The solution should be able to be executed independently.
# Call Mapper

## Overview
This project is designed to read an Excel file and collect data on its content. Initially, it focuses on scanning API calls along with timestamps to facilitate testing by identifying erroneous or broken calls or timestamps. 
The functionality will be expanded in the future to accommodate a wider range of applications.

## Getting Started
To run this project, ensure you have Python installed on your system. See requirements.txt for dependencies.

## Configuration

Before running the script, please create a .env file in the root directory of the project and populate it with your specific API patterns and associated column names. 
This setup is crucial for the script to process data correctly according to your needs. 
Define each pattern and its corresponding columns as shown in the example below, adapting them to fit the API calls and data structure you intend to analyze.

Example variable setup in .env file:
```bash
DEFAULT_PATTERN="/api/example/pattern"
EXAMPLE_COLUMNS="column1,column2,column3"
```
## Running the Script
To execute the main script of this project, follow these steps:

1. Open a terminal or command prompt.
2. Navigate to the directory where the project is located.
3. Run the script using Python by typing the following command:

```bash
python main.py
```
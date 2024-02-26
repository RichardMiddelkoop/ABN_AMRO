# ABN AMRO Programming Exercise

## Functional Description:
The application is used with two separate datasets of clients that is needed for interfacing with clients. One dataset contains information about the clients and the other one contains information about their financial details.

The application filters and combines the datasets, with a given country filter, to create an output containing emails of the clients in combination with some financial details.

## In- and Output:

- Application receives two arguments:
  1. The paths to each of the dataset files
  2. The countries to filter the data on.
- Saves output to **client_data** directory in the root directory of the project.
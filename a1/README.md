# CSC469 Assignment 1

## Authors
- Eugene Yue-Hin Cheung: cheun550
- Eric Snyder: snyderer


** Final revision: 256 **


## Part A

To run the experiment (including compilation):

```shell
$ ./run_experiment_A
```

Use `-h` to see descriptions of the available program arguments.


## Part B

To generate data and write to a CSV file:

```shell
$ ./partb_data -o data_dump.csv
```

The script be run multiple times, and results will be appended to the existing
output file (with only a single header at the top).

To generate a graphs from the data (as .eps files):

```shell
$ ./partb_plot -i data_dump.csv -o graph_files_prefix
```

Use `-h` to see descriptions of the available program arguments.

The data that was collected for Part B is included in `/data/partb_data.csv`.
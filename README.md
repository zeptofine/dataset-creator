# dataset-creator

This is a tool I made to assist making datasets for image models.

## Installation

The simplest way to install is to run

```bash
git clone https://github.com/zeptofine/dataset-creator
cd dataset-creator
```

, create a virtual environment, then

```bash
pip install -e .
```

inside.

## GUI

This gui is (currently) used to configure the actions of the
[imdataset_creator](imdataset_creator) main.
![dataset-creator-gui](github/images/dc_empty.png)
To run it, you can execute `python -m imdataset_creator.gui` (or if you installed it, `imdataset-creator-gui`) in the terminal. When you save, the config will normally appear in `<PWD>/config.json`. Make sure it doesn't overwrite anything important!

### Inputs

![dataset creator inputs window](github/images/dc_inputs.png)

The folder is what is searched through to find images, and the search patterns are used in [`wcmatch.glob`](https://facelessuser.github.io/wcmatch/glob/) to find files.

### Producers

![dataset creator producers window](github/images/dc_producers.png)

Rules use producers to get information about files. Of course, Rules themselves could gather this information themselves but that is a little inefficient when it comes to multiple consecutive runs. The data saved by the producers will be saved to a file, by default `filedb.arrow`.

### Rules

Rules are used to filter out unwanted files. For example, one of them restricts the resolution of allowed files to a certain range, and another restricts the modification time within a certain range.

![dataset creator rules window](github/images/dc_rules.png)

When a Rule needs a producer, the rule should tell you what it needs in its description. Pick the appropriate Producer in the Producers.

**!Neither Producers or Rules need to be defined for inputs/outputs to work!**

### Outputs & Filters

![dataset creator outputs window](github/images/dc_outputs.png)

Outputs have a folder, which is used to send created images, and the format_text is used to define files new paths. The `overwrite existing files` checkbox defines whether you overwrite existing files in the output folder if they already exist.

The `Filters` list show functions that will be applied to images going through this step. They can apply noise, compression, etc. to images.

## Running

To run the program, run `python -m imdataset_creator` (or if you installed it, `imdataset-creator`) in the terminal.

### Arguments

```rich
--config-path                              PATH     Where the dataset config is placed [default: config.json]
                                                    This is the config you create using the GUI.

--database-path                            PATH     Where the database is placed [default: filedb.arrow]
                                                    The database that the Producer save to.

--threads                                  INTEGER  multiprocessing threads [default: 9]
                                                    The number of separate processes to make in the execution step.
--chunksize                                INTEGER  imap chunksize [default: 5]
                                                    This can make a small performance difference. I personally don't see it much

                      -p                   INTEGER  chunksize when populating the df [default: 100]
                                                    This affects the efficiency of df population. Try to keep this high.

--simulate                --no-simulate             stops before conversion [default: no-simulate]
                                                    This is mainly for debugging. this exits right before execution.
                                                    
--verbose                 --no-verbose              prints converted files [default: no-verbose]

                      -s                   INTEGER  save interval in secs when populating the df [default: 60]
                                                    How often to autosave. By default, it autosaves every minute when it can

--help                                              Show this message and exit.
```

## TODO

- [x] make UI
- [x] redo config setup
- [ ] More filters
- [ ] bind UI to the CLI methods

Before the last point can be started, create_dataset.py must be broken down far enough such that almost all it is controlling is progress tracking.

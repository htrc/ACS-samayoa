# page-matches-extract

This tool finds mentions of a set of given keywords in a given set of volumes,
and extracts the pages that contain at least one mention of any of the input
keywords, as well as the counts representing the number of times each keyword
was found on that page.

## Building the code

`sbt clean compile test` -- to compile and run unit tests  
`sbt dist` -- to create a deployable ZIP package  

## Running the code

```
page-matches-extract 20210428T202932
HathiTrust Research Center
  -k, --keywords  <FILE>      The path to the file containing the keyword
                              patterns (one per line) to count
  -l, --log-level  <LEVEL>    (Optional) The application log level; one of INFO,
                              DEBUG, OFF (default = INFO)
  -c, --num-cores  <N>        (Optional) The number of CPU cores to use (if not
                              specified, uses all available cores)
  -n, --num-partitions  <N>   (Optional) The number of partitions to split the
                              input set of HT IDs into, for increased
                              parallelism
  -o, --output  <DIR>         Write the output to DIR (should not exist, or be
                              empty)
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --spark-log  <FILE>     (Optional) Where to write logging output from
                              Spark to
  -h, --help                  Show help message
  -v, --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```

Example:  
```shell
./bin/page-matches-extract \
    -J-Xmx4g \
    -p /path/to/source/pairtree/volumes/folder/ \
    -o /output/location/ \
    -k /path/to/patterns.tsv \
    /path/to/volume/ids.txt
```

Example `patterns.tsv` file:
```text
AAPI    (?<=^|[\s("])(?:AAPI(?:s|S)?\b|A\.A\.P\.I\.\B)
aboriginal      (?i)\baboriginals?\b
affirmative_action      (?i)\baffirmative\saction\b
african_american        (?i)\bafrican(-|\s)?americ\p{L}+\b|\b(afro-?americ\p{L}+)|(afro\sameric\p{L}+)\b
alaskan (?i)\balaskans?\b
albanian        (?i)\balbanians?\b
aleut   (?i)\baleuts?\b
alien_person    (?i)\b(?:person|people)\b(?:\p{P}?\s?\S+\b){0,3}.alien\b|\balien\b(?:\p{P}?\s?\S+\b){0,3}.(?:person|people)\b
AANAPISI        (?<=^|[\s("])(?:AANAPISI(?:s|S)?\b|A\.A\.N\.A\.P\.I\.S\.I\.\B)|(?i)\basian\samerican\snative\samerican\spacific\sislander\sserving\b
argentinean     (?i)\bargentinians?\b
armenian        (?i)\barmenians?\b
```
where the regular expressions in the second column (TAB-separated) indicate the
search criteria that will be used to find matches that will be counted under the 
heading specified in the first column.

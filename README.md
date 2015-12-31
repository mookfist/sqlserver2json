# sqlserver2json

Very simple script to convert a SQL server database to json files, one file per table. Probably not going to be great with very large tables.

# json output

Attempst to find the primary key of the table and if it can, will use its value as a key in an object literal.

If it can't find the primary key, then each row will be in an array rather than an object.

## Installation

```
$ git clone https://github.com/mookfist/sqlserver2json.git
$ npm install
$ chmod u+x bin/sqlserver2json.js
$ bin/sqlserver2json.js --help
```

## Testing

You can use --test to do a syntax check on the json files. This is memory intensive. Tables larger than 4gb might cause problems.

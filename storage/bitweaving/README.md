# BitWeaving
A BitWeaving Index for Quickstep.

See [the paper](http://pages.cs.wisc.edu/~jignesh/publ/BitWeaving.pdf) for an indepth look.

This is an optional submodule for Quickstep. It **must be kept private** because of restrictions related to a patent. That said, if you are a member of UWQuickstep, feel free to try it. It installs like any submodule. Just be careful to remove it the correct way (described below) otherwise git will be angry with you.

## Installing
This assumes you have a `build/` directory with quickstep built in it.
```bash
cd quickstep_root

git submodule add https://github.com/UWQuickstep/bitweaving.git storage/bitweaving
# Triggers a rebuild of the CMakefile
touch CMakeLists.txt
cd build && make -j4
```
## Usage
The Bitweaving index can be created like any other index. However, it must be made on a compressed block.
``` SQL
create table foo (id int, name varchar(20)) with
  blockproperties (type compressed_columnstore, compress all, sort id);

create index bwfoo on foo(id) using bitweaving;

copy foo from 'foo.csv' with (delimiter ',');

-- A bitweaving index has now been created on foo.

```

A variant of the default bitweaving index exists. To build the Horizontal variant, use this syntax:

``` SQL
create index bwfoo on foo(id) using bitweaving (TYPE H);
-- Note: the default bitweaving type is specified as  ...using bitweaving (TYPE V);
```

## Uninstalling
The removal process is involved.
``` bash
cd quickstep_root

git submodule deinit -f storage/bitweaving
git rm storage/bitweaving --cached
rm storage/bitweaving -rf
rm -rf .git/modules/storage -rf
git checkout .gitmodules
touch CMakeLists.txt
cd build && make -j4

```

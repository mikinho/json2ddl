# json2ddl
Quick Python script to Infer SQL schema from JSON, including nested-array subtables and lists of primitives.

## Usage

```./json2ddl.py [-h] -i INPUT [-t TABLE] [-o OUTPUT] [--primary-key PRIMARY_KEY] [-c CUSHION] [--pascal] [--nerd] [--sort] [-m [MAP ...]]

Infer SQL schema from JSON

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        JSON file path
  -t TABLE, --table TABLE
                        Root table name (from filename if omitted)
  -o OUTPUT, --output OUTPUT
                        Output SQL file
  --primary-key PRIMARY_KEY, --pk PRIMARY_KEY
                        Source column for PRIMARY KEY
  -c CUSHION, --cushion CUSHION
                        Cushion percent or absolute value
  --pascal              PascalCase names
  --nerd                Power-of-two VARCHAR sizing
  --sort                Group non-null column groups first, grouping bases
  -m [MAP ...], --map [MAP ...]
                        Column rename mappings src:dst```

# json2ddl
Quick Python script to Infer SQL schema from JSON, including nested-array subtables and lists of primitives.

## Install

Download and then chmod +x json2ddl.py

## Notes

- Subtable syntax kind of sucks but will improve.
- This isn't perfect and isn't meant to be used for a final schema but it should help you get started

## Usage

```
./json2ddl.py [-h] -i INPUT [-t TABLE] [-o OUTPUT] [--primary-key PRIMARY_KEY] [-c CUSHION] [--pascal] [--nerd] [--sort] [-m [MAP ...]]

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
## Example

```json
[
    {
        "recId": "123",
        "NAME1": "Michael Welter",
        "NAME2": "",
        "NAME3": "",
        "ADDRESS1": "SOME STREET",
        "ADDRESS2": "SOME SUITE",
        "CITY": "HOUSTON",
        "STATE": "TX",
        "COUNTRY": "US",
        "ZIP": "777004"
    }
]
```
```
./json2ddl.py --input NAME.json --pascal --pk recId --map recId:\$tableID --sort --nerd --cushion 25
```

```sql
CREATE TABLE Name (
    "NameID" VARCHAR(8) PRIMARY KEY,
    "Name1" VARCHAR(64) NOT NULL,
    "Name2" VARCHAR(96),
    "Name3" VARCHAR(48),
    "Address1" VARCHAR(64),
    "Address2" VARCHAR(48),
    "City" VARCHAR(48),
    "Country" VARCHAR(4),
    "State" VARCHAR(2),
    "Zip" VARCHAR(16)
);
```

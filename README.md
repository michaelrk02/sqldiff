# sqldiff

A Go-based tool to compare the contents of two SQL tables

## Usage

```
$ go run main.go
  -keys string
        primary keys (comma-separated)
  -left string
        left connection name
  -patch string
        patch options: (i)nsert, (u)pdate, (d)elete
  -right string
        right connection name
  -strategy string
        compare strategy (keys/all) (default "keys")
  -table string
        table to compare
  -tee
        output to file
```

Examples:

- `go run main.go -left his_v3_staging -right his_v3 -table value_set -keys name,code_system,code`
- `go run main.go -left his_v3_staging -right his_v3 -table healthcare_service -keys healthcare_service_id -tee -strategy all -patch iud`
